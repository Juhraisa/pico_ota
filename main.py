"""
=============================================================================
 SAHKO- JA KODINOHJAUSJARJESTELMA - Raspberry Pi Pico W / MicroPython
=============================================================================

Mita tama tekee:
  - Asynkroninen (asyncio) web-palvelin porttiin 80.
  - Hakee kellonajan NTP:sta ja laskee Suomen kesa-/talviajan itse.
  - Hakee pörssisähkön hinnat osoitteesta api.spot-hinta.fi (TLS, oma
    kevyt async HTTP-asiakas - ei ulkoista urequests-riippuvuutta).
  - Näyttää nykyisen hinnan (snt/kWh) seka tämän ja seuraavan vuorokauden
    tuntihinnat pylväsdiagrammeina, joissa jokaisessa pylväässä lukee arvo.
  - GPIO-pinnit 14-21 releohjauksiin, kullekin oma nimi, manuaalinen
    päälle/pois-kytkin, ajastus, hintakattoautomatiikka ja (makuuhuone +
    olohuone) lämpötila-automatiikka.
  - Lukee huoneiden lämpötilat suoraan RuuviTag-antureiden BLE-mainos-
    paketeista (aioble) - ei enää MQTT-välittäjää. Kaikkien BLE_TAG_ROOMS-
    kartassa nimettyjen huoneiden lämpötilat näytetään sivun ylälaidassa.
  - Näyttää sivun ylälaidassa myös reaaliaikaisen kokonaistehon paikalliselta
    Shelly EM3 -mittarilta (SHELLY_HOST, tavallinen HTTP /status-kysely).
  - OTA-päivitys: "Ohjelmistopäivitys"-napista laite hakee GitHub-repostasi
    (OTA_HOST+OTA_PATH_PREFIX) uusimman main.py:n, jos version.txt poikkeaa
    APP_VERSIONista, ja ottaa vanhasta varmuuskopion (main_prev.py) ennen
    käyttöönottoa ja uudelleenkäynnistystä.
  - Asetukset (ajastukset, hintarajat, lämpötilatavoitteet) tallentuvat
    tiedostoon /settings.json ja palautuvat uudelleenkäynnistyksen jälkeen.

Vaaditut riippuvuudet (asenna PC:ltä mpremote-ohjelmalla, USB kiinni):

    mpremote mip install aioble

  (ntptime on useimmissa Pico W -firmwareissa jo mukana; jos ei, se voi
  asentaa komennolla "mpremote mip install ntptime". BLE vaatii Pico W:n
  firmwaren, jossa Bluetooth-tuki on mukana - jos "import aioble" antaa
  ImportError:in, hae uusi .uf2 osoitteesta https://micropython.org/download/RPI_PICO_W/
  ja varmista etta valittu build sisaltaa BLE-tuen.)

Vaatii MicroPython-firmwaren version >= 1.22.0 (asyncio+TLS-tuki).
Päivitä uusin .uf2 osoitteesta https://micropython.org/download/RPI_PICO_W/
jos update tarvitaan.

Muokkaa alla oleva ASETUKSET-osio omaan ympäristöösi sopivaksi ja tallenna
tiedosto Pico W:lle nimellä main.py, niin ohjelma käynnistyy automaattisesti.

HUOM: Web-palvelimessa ei ole kirjautumista/salausta - se on tarkoitettu
käytettäväksi vain luotetussa kotiverkossa, ei internetiin avattuna.

Oletuksia jotka on tehty puolestasi (muuta tarvittaessa koodista):
  - Hintana käytetään verollista hintaa (PriceWithTax).
  - Releet ovat oletuksena "active high" (1 = päällä). Jos korttisi on
    active-low, aseta RELAY_ACTIVE_HIGH = False.
  - Lämpötilaohjaus on lämmitys-logiikka: pinni menee päälle kun huone on
    liian kylmä ja pois kun tavoitelämpötila+hystereesi on saavutettu.
  - Jos pinnille on päällä useampi automatiikka yhtä aikaa, tärkeysjärjestys
    on: lämpötila > hintakatto > ajastus > manuaalinen ohjaus.
=============================================================================
"""

try:
    import uasyncio as asyncio
except ImportError:
    import asyncio

import machine
import network
import time
import json
import gc
import struct
import aioble
import os
from micropython import const

# =============================================================================
# ASETUKSET - MUOKKAA NAMA OMAAN YMPARISTOOSI SOPIVIKSI
# =============================================================================

WIFI_SSID = "x"
WIFI_PASSWORD = "h1rvensalo!"

NTP_HOST = "fi.pool.ntp.org"

# Huoneiden lampotilat luetaan RuuviTagien BLE-mainospaketeista (katso
# BLE_TAG_ROOMS alempana "PINNIT JA HUONEET" -osiossa, jossa jokaiselle
# tagille annetaan MAC-osoite ja huone).

RELAY_ACTIVE_HIGH = True           # False jos relekortti on active-low

TEMP_HYSTERESIS = 0.2              # astetta, lampotila-automatiikan kuollut alue
TEMP_MAX_AGE = 30 * 60             # s, tata vanhempaa mittausta ei kayteta

# RuuviTag mainostaa lampotilan n. sekunnin valein, joten BLE-skannauksen ei
# tarvitse kuunnella jatkuvasti (100% duty cycle syo muistia/radioaikaa WiFilta
# ja hidastaa web-palvelinta). 200ms ikkuna joka 1s (20%) riittaa hyvin, koska
# sivu paivittyy vain 15s valein joka tapauksessa. Nosta BLE_SCAN_WINDOW_US:aa
# jos lampotilat tuntuvat paivittyvan liian harvoin.
BLE_SCAN_INTERVAL_US = 1000000     # us, kuinka usein skannausjakso alkaa (1s)
BLE_SCAN_WINDOW_US = 200000        # us, kuinka pitkan osan jaksosta oikeasti kuunnellaan

# TARKEA: aioble.scan()-kutsun duration_ms EI SAA olla 0 (=ikuinen skannaus).
# Se on tunnettu aioblen muistivuoto nimenomaan Pico W:lla - muutama tavu per
# vastaanotettu mainos jaa keraytymatta, mika nakyy gc.collect()-jalkeisen
# vapaan muistin hitaana laskuna tuntien saatossa (katso
# https://github.com/orgs/micropython/discussions/15684). Aarellinen kesto
# silmukassa (skannaa N ms, lopeta, aloita heti uusi) valttaa vuodon kokonaan.
BLE_SCAN_DURATION_MS = 5000        # ms, yhden skannausistunnon pituus ennen uudelleenkaynnistysta

PRICE_FETCH_HOUR = 14              # klo (paikallista aikaa), jolloin hinnat haetaan paivittain
PRICE_FETCH_MINUTE = 20            # min
AUTOMATION_INTERVAL = 20           # s, kuinka usein automatiikka tarkistetaan
NTP_RESYNC_INTERVAL = 6 * 60 * 60  # s

# Shelly EM3:n oma IP paikallisverkossa (nakyy laitteen omasta /status-
# vastauksesta kohdasta wifi_sta.ip). Kannattaa varata talle reitittimesta
# kiintea/staattinen osoite (DHCP-reservointi), etta tama ei paty rikki jos
# reititin joskus jakaa laitteelle eri osoitteen.
SHELLY_HOST = "192.168.88.196"
SHELLY_POLL_INTERVAL = 15          # s, kuinka usein tehonlukema haetaan (sama tahti kuin sivu paivittyy)
SHELLY_MAX_AGE = 120                # s, tata vanhempi lukema merkitaan vanhentuneeksi

HTTP_PORT = 80

SPOT_HOST = "api.spot-hinta.fi"
SPOT_PATH = "/TodayAndDayForward?priceResolution=60"

SETTINGS_FILE = "/settings.json"

# Nosta APP_VERSION jokaisen julkaistavan main.py-muutoksen yhteydessa. Push
# muuttunut main.py JA taman lukeman kanssa yhta suureksi paivitetty
# version.txt samaan repoon - laite vertailee vain version.txt:ta ennen kuin
# lataa koko main.py:n, jottei jokainen tarkistus lataisi turhaan 50+ kt.
APP_VERSION = "260920"
OTA_HOST = "raw.githubusercontent.com"
OTA_PATH_PREFIX = "/Juhraisa/pico_ota/main"   # {OTA_HOST}{OTA_PATH_PREFIX}/version.txt ja /main.py
OTA_MAIN_PATH = "/main.py"          # kaynnissa oleva ohjelma
OTA_BACKUP_PATH = "/main_prev.py"   # edellinen toimiva versio, kasin palautettavissa USB:lla
OTA_STAGING_PATH = "/main_new.py"   # tahan ladataan uusi versio ennen kayttoonottoa

# =============================================================================
# PINNIT JA HUONEET
# =============================================================================

PIN_NUMBERS = (14, 15, 16, 17, 18, 19, 20, 21)

PIN_NAMES = {
    14: "Vapaa",
    15: "Eteinen",
    16: "Kylpyhuone",
    17: "Makuuhuone",
    18: "Tyohuone",
    19: "Ulkovalo",
    20: "Olohuone",
    21: "Lamminvesivaraaja",
}

# Pinnit joilla on huoneen lampotilaan perustuva automatiikka
TEMP_ROOM_FOR_PIN = {17: "makuuhuone", 20: "olohuone"}

# RuuviTagin MAC-osoite -> huoneavain (sama nimiavaruus kuin TEMP_ROOM_FOR_PIN
# arvot). Kirjainkoolla ei ole valia. Uuden tagin voi lisata olemassa olevaan
# huoneeseen suoraan tahan - jos taas haluat kokonaan uuden huoneen sivun
# ylalaitaan, lisaa se seka tahan etta BLE_ROOM_LABELS- ja BLE_ROOM_ORDER-
# listoihin alla.
BLE_TAG_ROOMS = {
    "D6:3E:35:C5:18:4E": "lastenhuone",
    "F6:7F:92:96:A3:AE": "makuuhuone",
    "FB:97:CF:18:8C:DE": "olohuone",
    "F6:8A:3D:10:32:E5": "eteinen",
}
_BLE_TAG_ROOMS_LOWER = {mac.lower(): room for mac, room in BLE_TAG_ROOMS.items()}

# Huoneavain -> naytettava nimi
BLE_ROOM_LABELS = {
    "olohuone": "Olohuone",
    "makuuhuone": "Makuuhuone",
    "lastenhuone": "Lastenhuone",
    "eteinen": "Eteinen",
}

# Nayttojarjestys sivun ylalaidassa (olohuone ja makuuhuone ensin, kuten ennenkin)
BLE_ROOM_ORDER = ["olohuone", "makuuhuone", "lastenhuone", "eteinen"]

# =============================================================================
# GPIO-ALUSTUS
# =============================================================================

pin_objs = {}
for _p in PIN_NUMBERS:
    pin_objs[_p] = machine.Pin(_p, machine.Pin.OUT, value=(0 if RELAY_ACTIVE_HIGH else 1))


def set_pin(num, on):
    """Asettaa releen fyysisen tilan ja paivittaa tilatiedon."""
    val_on = 1 if RELAY_ACTIVE_HIGH else 0
    val_off = 0 if RELAY_ACTIVE_HIGH else 1
    pin_objs[num].value(val_on if on else val_off)
    pins[num]["state"] = bool(on)


# =============================================================================
# TILA (RUNTIME STATE)
# =============================================================================

def default_pin_state(num):
    return {
        "name": PIN_NAMES[num],
        "state": False,
        "sched_enabled": False,
        "sched_on": "07:00",
        "sched_off": "22:00",
        "price_enabled": False,
        "price_limit": 8.0,
        "temp_enabled": False,
        "temp_target": 21.0,
        "temp_capable": num in TEMP_ROOM_FOR_PIN,
        "auto_mode": None,
    }


pins = {p: default_pin_state(p) for p in PIN_NUMBERS}

temps = {room: {"value": None, "updated": 0} for room in BLE_ROOM_ORDER}
shelly_power = {"value": None, "updated": 0}

wifi_status = {"connected": False}
ble_status = {"active": False}
ota_status = {"checking": False, "message": ""}
wlan = None

prices_today = []       # [(tunti, snt/kWh), ...]
prices_tomorrow = []
prices_today_date = None      # paivamaara-str jota prices_today edustaa
prices_tomorrow_date = None   # paivamaara-str jota prices_tomorrow edustaa
price_now = None
prices_valid = False
prices_updated_at = 0

ntp_synced = False
ntp_last_sync = 0


def get_temp(room):
    d = temps.get(room)
    if not d or d["value"] is None:
        return None, True
    stale = (time.time() - d["updated"]) > TEMP_MAX_AGE
    return d["value"], stale


def get_shelly_power():
    d = shelly_power
    if d["value"] is None:
        return None, True
    stale = (time.time() - d["updated"]) > SHELLY_MAX_AGE
    return d["value"], stale


# =============================================================================
# ASETUSTEN PYSYVYYS (JSON-tiedosto flashissa)
# =============================================================================

SETTINGS_KEYS = (
    "sched_enabled", "sched_on", "sched_off",
    "price_enabled", "price_limit",
    "temp_enabled", "temp_target",
)


def save_settings():
    try:
        data = {}
        for p in PIN_NUMBERS:
            data[str(p)] = {k: pins[p][k] for k in SETTINGS_KEYS}
        with open(SETTINGS_FILE, "w") as f:
            json.dump(data, f)
    except Exception as e:
        print("Asetusten tallennus epaonnistui:", e)


def load_settings():
    try:
        with open(SETTINGS_FILE) as f:
            data = json.load(f)
        for p in PIN_NUMBERS:
            saved = data.get(str(p))
            if saved:
                for k in SETTINGS_KEYS:
                    if k in saved:
                        pins[p][k] = saved[k]
        print("Asetukset ladattu tiedostosta", SETTINGS_FILE)
    except Exception as e:
        print("Ei aiempia tallennettuja asetuksia (tai virhe):", e)


# =============================================================================
# AIKA JA SUOMEN KESA-/TALVIAIKA (ei ulkoisia riippuvuuksia)
# =============================================================================

_DAYS_IN_MONTH = (31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)


def _is_leap(y):
    return (y % 4 == 0 and y % 100 != 0) or (y % 400 == 0)


def _days_from_civil(y, m, d):
    """Paivien maara 2000-01-01:sta annettuun paivamaaraan (voi olla negat.)."""
    days = 0
    if y >= 2000:
        for yy in range(2000, y):
            days += 366 if _is_leap(yy) else 365
    else:
        for yy in range(y, 2000):
            days -= 366 if _is_leap(yy) else 365
    for mm in range(1, m):
        days += 29 if (mm == 2 and _is_leap(y)) else _DAYS_IN_MONTH[mm - 1]
    days += d - 1
    return days


def _weekday(y, m, d):
    # 2000-01-01 oli lauantai. Palautetaan Mon=0 ... Sun=6 (lauantai=5).
    return (_days_from_civil(y, m, d) + 5) % 7


def _last_sunday(y, month):
    dim = 29 if (month == 2 and _is_leap(y)) else _DAYS_IN_MONTH[month - 1]
    wd = _weekday(y, month, dim)
    return dim - ((wd - 6) % 7)


def dst_offset(utc_epoch):
    """Palauttaa UTC-poikkeaman tunteina (2=talviaika, 3=kesaaika) EU-saannolla."""
    t = time.localtime(utc_epoch)
    y, m, d, hh = t[0], t[1], t[2], t[3]
    mar_sun = _last_sunday(y, 3)
    oct_sun = _last_sunday(y, 10)
    start = (3, mar_sun, 1)
    end = (10, oct_sun, 1)
    cur = (m, d, hh)
    return 3 if start <= cur < end else 2


def local_now():
    """Palauttaa time.localtime()-tyyppisen tuplen Suomen paikallisajassa."""
    epoch = time.time()
    off = dst_offset(epoch)
    return time.localtime(epoch + off * 3600)


def local_date_str(offset_days=0):
    epoch = time.time() + dst_offset(time.time()) * 3600 + offset_days * 86400
    t = time.localtime(epoch)
    return "%04d-%02d-%02d" % (t[0], t[1], t[2])


def _seconds_until(hour, minute):
    """Sekunteja seuraavaan ajankohtaan HH:MM (paikallista aikaa). Jos ajankohta
    on jo tanaan ohi, palautetaan aika huomiseen samaan kellonaikaan. Lasketaan
    aina tuoreesta local_now()-kutsusta, joten kesa-/talviajan vaihtuminen ja
    NTP-korjaukset eivat aiheuta ajautumista."""
    lt = local_now()
    now_s = lt[3] * 3600 + lt[4] * 60 + lt[5]
    target_s = hour * 3600 + minute * 60
    diff = target_s - now_s
    if diff <= 0:
        diff += 86400
    return diff


# =============================================================================
# KEVYT ASYNKRONINEN HTTP(S)-GET (ei urequests-riippuvuutta)
# =============================================================================

async def http_get(host, path, port=443, ssl=True, timeout=15):
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, port, ssl=ssl), timeout
    )
    try:
        req = (
            "GET %s HTTP/1.1\r\n"
            "Host: %s\r\n"
            "User-Agent: picow-sahko/1.0\r\n"
            "Accept: application/json\r\n"
            "Connection: close\r\n\r\n"
        ) % (path, host)
        writer.write(req.encode())
        await writer.drain()

        status_line = await asyncio.wait_for(reader.readline(), timeout)
        if not status_line:
            raise OSError("Tyhja vastaus palvelimelta")
        parts = status_line.split(b" ", 2)
        code = int(parts[1]) if len(parts) > 1 else 0

        content_length = None
        chunked = False
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout)
            if line in (b"\r\n", b"\n", b""):
                break
            low = line.lower()
            if low.startswith(b"content-length:"):
                content_length = int(line.split(b":", 1)[1].strip())
            elif low.startswith(b"transfer-encoding:") and b"chunked" in low:
                chunked = True

        if code != 200:
            raise OSError("HTTP-virhe %d" % code)

        if chunked:
            parts_list = []
            while True:
                size_line = (await asyncio.wait_for(reader.readline(), timeout)).strip()
                if not size_line:
                    continue
                size = int(size_line.split(b";")[0], 16)
                if size == 0:
                    await asyncio.wait_for(reader.readline(), timeout)
                    break
                chunk = await asyncio.wait_for(reader.readexactly(size), timeout)
                parts_list.append(chunk)
                await asyncio.wait_for(reader.readline(), timeout)
            return b"".join(parts_list)
        elif content_length is not None:
            return await asyncio.wait_for(reader.readexactly(content_length), timeout)
        else:
            chunks = []
            while True:
                c = await asyncio.wait_for(reader.read(1024), timeout)
                if not c:
                    break
                chunks.append(c)
            return b"".join(chunks)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


# =============================================================================
# PORSSISAHKON HINTA
# =============================================================================

def _parse_entry_dt(s):
    """'2026-08-12T14:00:00+03:00' -> (paivamaara-str, minuutti vuorokaudessa)."""
    date_str = s[0:10]
    hh = int(s[11:13])
    mm = int(s[14:16])
    return date_str, hh * 60 + mm


def _aggregate_hourly(entries):
    """entries: [(minuutti, snt/kWh), ...] -> [(tunti, keskiarvo_snt), ...]."""
    sums = [0.0] * 24
    counts = [0] * 24
    for minu, price in entries:
        h = minu // 60
        if 0 <= h < 24:
            sums[h] += price
            counts[h] += 1
    out = []
    for h in range(24):
        if counts[h]:
            out.append((h, round(sums[h] / counts[h], 2)))
    return out


def _roll_over_day_if_needed():
    """Jos kalenteripaiva on vaihtunut edellisen hintahaun jalkeen, siirretaan jo
    haettu 'huominen'-data 'tanaan'-dataksi ilman uutta verkkohakua. Jos sopivaa
    valmiiksi haettua dataa ei ole (esim. haku on epaonnistunut kahdesti peräkkäin),
    prices_today tyhjennetaan ja prices_valid asetetaan epatodeksi, jottei vanhaa
    paivaa naytettaisi virheellisesti tamanpaivaisena."""
    global prices_today, prices_tomorrow, prices_today_date, prices_tomorrow_date, prices_valid
    today_str = local_date_str(0)
    if today_str == prices_today_date:
        return
    if today_str == prices_tomorrow_date and prices_tomorrow:
        prices_today = prices_tomorrow
        prices_today_date = prices_tomorrow_date
        prices_tomorrow = []
        prices_tomorrow_date = None
        print("Vuorokausi vaihtui - kaytetaan valmiiksi haettua dataa tanaan-datana")
    else:
        prices_today = []
        prices_today_date = None
        prices_valid = False
        print("Vuorokausi vaihtui, mutta uutta dataa ei ole viela saatavilla")


def update_price_now():
    """Paivittaa price_now-muuttujan valimuistissa olevasta prices_today-taulukosta
    ilman verkkohakua. Kutsutaan seka onnistuneen hintahaun jalkeen etta saannollisesti
    automatiikan ja web-rajapinnan toimesta, jotta 'nykyinen hinta' pysyy oikeana
    myos niina tunteina jolloin ei haeta uutta dataa netista."""
    global price_now
    _roll_over_day_if_needed()
    if not prices_today:
        price_now = None
        return
    lt = local_now()
    h = lt[3]
    best = None
    for hour, pr in prices_today:
        if hour <= h:
            best = pr
        else:
            break
    price_now = best if best is not None else prices_today[0][1]


async def update_prices():
    global prices_today, prices_tomorrow, prices_today_date, prices_tomorrow_date
    global prices_valid, prices_updated_at
    try:
        raw = await http_get(SPOT_HOST, SPOT_PATH)
        data = json.loads(raw)
        del raw
        gc.collect()

        by_date = {}
        for item in data:
            try:
                dstr, minu = _parse_entry_dt(item["DateTime"])
                price_snt = item["PriceWithTax"] * 100.0
            except Exception:
                continue
            by_date.setdefault(dstr, []).append((minu, price_snt))
        del data
        gc.collect()

        today_str = local_date_str(0)
        tomorrow_str = local_date_str(1)

        today_entries = sorted(by_date.get(today_str, []))
        tomorrow_entries = sorted(by_date.get(tomorrow_str, []))

        prices_today = _aggregate_hourly(today_entries)
        prices_tomorrow = _aggregate_hourly(tomorrow_entries)
        prices_today_date = today_str
        prices_tomorrow_date = tomorrow_str if prices_tomorrow else None
        prices_valid = True
        prices_updated_at = time.time()

        update_price_now()

        print("Hintatiedot paivitetty:", today_str, "-", len(today_entries),
              "tanaan,", len(tomorrow_entries), "huomenna")
    except Exception as e:
        print("Hintojen haku epaonnistui:", e)
    gc.collect()


async def price_update_task():
    await update_prices()
    while True:
        wait_s = _seconds_until(PRICE_FETCH_HOUR, PRICE_FETCH_MINUTE)
        print("Seuraava hintahaku %d s kuluttua (klo %02d:%02d)" %
              (wait_s, PRICE_FETCH_HOUR, PRICE_FETCH_MINUTE))
        await asyncio.sleep(wait_s)
        await update_prices()


# =============================================================================
# SHELLY EM3 - REAALIAIKAINEN SAHKONKULUTUS (paikallisverkko, ei TLS:aa)
# =============================================================================

async def shelly_power_task():
    """Hakee Shelly EM3:n hetkellisen kokonaistehon SHELLY_HOST:n /status-
    rajapinnasta saannollisesti ja paivittaa shelly_power-tilan. Sama laite on
    paikallisverkossa, joten tavallinen HTTP portissa 80 riittaa - TLS:aa ei
    tarvita toisin kuin porssisahkon haussa."""
    while True:
        try:
            body = await http_get(SHELLY_HOST, "/status", port=80, ssl=False, timeout=5)
            data = json.loads(body)
            shelly_power["value"] = round(float(data["total_power"]) / 1000, 2)
            shelly_power["updated"] = time.time()
        except Exception as e:
            print("Shellyn tehonlukeman haku epaonnistui:", e)
        await asyncio.sleep(SHELLY_POLL_INTERVAL)


# =============================================================================
# NTP-AJAN SYNKRONOINTI
# =============================================================================

try:
    import ntptime
    ntptime.host = NTP_HOST
except ImportError:
    ntptime = None


async def ntp_sync_task():
    global ntp_synced, ntp_last_sync
    if ntptime is None:
        print("ntptime-moduulia ei loydy - kello ei synkronoidu automaattisesti")
        return
    while True:
        try:
            ntptime.settime()
            ntp_synced = True
            ntp_last_sync = time.time()
            print("NTP-aika synkronoitu")
        except Exception as e:
            print("NTP-synkronointi epaonnistui:", e)
        await asyncio.sleep(NTP_RESYNC_INTERVAL)


# =============================================================================
# BLE (aioble) - RUUVITAGIEN LAMPOTILOJEN LUKEMINEN
# =============================================================================
# RuuviTagit lahettavat lampotilan BLE-mainospaketeissa (advertisement), joten
# yhteytta tageihin ei tarvita - aioble kuuntelee mainoksia taustalla ja
# skannaus jatkuu jatkuvasti (duration_ms=0). Korvaa aiemman MQTT-tilauksen.

_RUUVI_COMPANY_ID = const(0x0499)


def parse_ruuvi_v5(data):
    """Purkaa RuuviTagin Data Format 5:n (RAWv2) sensoridatasta lampotilan."""
    if len(data) < 24 or data[0] != 5:
        return None
    temp_c = struct.unpack(">h", data[1:3])[0] * 0.005
    return {"temp_c": round(temp_c, 2)}


async def ble_scan_task():
    """Skannaa jatkuvasti RuuviTageja ja paivittaa temps-sanakirjan BLE:n kautta.

    Skannaus tehdaan tarkoituksella lyhyissa BLE_SCAN_DURATION_MS:n pituisissa
    patkissa yhden ikuisen istunnon sijaan, koska aioble.scan(duration_ms=0)
    vuotaa muistia Pico W:lla (katso BLE_SCAN_DURATION_MS:n kommentti ylla).
    Kerayy roskat myos saannollisesti mainosten seassa, koska jatkuva skannaus
    tuottaa paljon pienia, lyhytikaisia olioita (yksi jokaista lahietyvaa BLE-
    laitetta kohti, ei vain RuuviTageja)."""
    seen_count = 0
    while True:
        try:
            async with aioble.scan(BLE_SCAN_DURATION_MS, interval_us=BLE_SCAN_INTERVAL_US,
                                    window_us=BLE_SCAN_WINDOW_US, active=False) as scanner:
                ble_status["active"] = True
                async for result in scanner:
                    try:
                        room = _BLE_TAG_ROOMS_LOWER.get(result.device.addr_hex().lower())
                        if room is None:
                            continue
                        for company_id, data in result.manufacturer(filter=_RUUVI_COMPANY_ID):
                            reading = parse_ruuvi_v5(data)
                            if reading:
                                temps[room]["value"] = reading["temp_c"]
                                temps[room]["updated"] = time.time()
                    except Exception as e:
                        print("Yksittaisen BLE-mainoksen kasittely epaonnistui:", e)

                    seen_count += 1
                    if seen_count >= 40:
                        seen_count = 0
                        gc.collect()
        except Exception as e:
            ble_status["active"] = False
            print("BLE-skannaus keskeytyi, yritetaan uudelleen 5s kuluttua:", e)
            await asyncio.sleep(5)


# =============================================================================
# AUTOMATIIKKA: AJASTUS / HINTAKATTO / LAMPOTILA
# =============================================================================

def in_schedule_window(now_minu, on_str, off_str):
    try:
        oh, om = on_str.split(":")
        fh, fm = off_str.split(":")
        on_m = int(oh) * 60 + int(om)
        off_m = int(fh) * 60 + int(fm)
    except Exception:
        return False
    if on_m == off_m:
        return False
    if on_m < off_m:
        return on_m <= now_minu < off_m
    else:  # ajastus menee keskiyon yli (esim. ulkovalo 20:00 - 07:00)
        return now_minu >= on_m or now_minu < off_m


def evaluate_all_pins():
    lt = local_now()
    now_minu = lt[3] * 60 + lt[4]
    fresh_prices = prices_valid and price_now is not None

    for p in PIN_NUMBERS:
        cfg = pins[p]
        mode = None
        desired = cfg["state"]  # oletus: sailyta manuaalisesti asetettu tila

        if cfg["temp_capable"] and cfg["temp_enabled"]:
            mode = "temp"
            room = TEMP_ROOM_FOR_PIN[p]
            val, stale = get_temp(room)
            if val is not None and not stale:
                target = cfg["temp_target"]
                if val <= target - TEMP_HYSTERESIS:
                    desired = True
                elif val >= target + TEMP_HYSTERESIS:
                    desired = False
                # valissa (hystereesi): sailytetaan nykyinen tila
        elif cfg["price_enabled"]:
            mode = "price"
            if fresh_prices:
                desired = price_now <= cfg["price_limit"]
        elif cfg["sched_enabled"]:
            mode = "sched"
            desired = in_schedule_window(now_minu, cfg["sched_on"], cfg["sched_off"])

        cfg["auto_mode"] = mode
        if mode is not None and desired != cfg["state"]:
            set_pin(p, desired)


async def automation_task():
    while True:
        try:
            update_price_now()
            evaluate_all_pins()
        except Exception as e:
            print("Automatiikan paivitys epaonnistui:", e)
        await asyncio.sleep(AUTOMATION_INTERVAL)


# =============================================================================
# OTA-PAIVITYS (hakee uusimman main.py:n GitHub-repostasi)
# =============================================================================
# Periaate: 1) haetaan ensin vain pieni version.txt ja verrataan APP_VERSIONiin,
# 2) jos eri, ladataan main.py SUORAAN TIEDOSTOON (ei RAM-puskuriin - tiedosto
# on kymmenia kilotavuja, eika sita haluta kokonaisena muistiin nykyisen
# muistitilanteen paalle), 3) tarkistetaan etta tiedosto nayttaa jarkevalta
# (koko + kelpaako se Python-syntaksiltaan), 4) vasta sitten vanha main.py
# siirretaan talteen nimella main_prev.py ja uusi otetaan kayttoon, minka
# jalkeen laite kaynnistetaan uudelleen. main_prev.py EI koskaan poisteta
# automaattisesti onnistumisen jalkeenkaan - jos uusi versio ei toimi, se
# palautetaan kasin (mpremote cp :main_prev.py :main.py) USB:n kautta.

async def ota_check_version():
    """Hakee version.txt:n sisallon (muutama tavu, kevyt haku)."""
    body = await http_get(OTA_HOST, OTA_PATH_PREFIX + "/version.txt", timeout=10)
    return body.decode().strip()


async def ota_download_to_file(path, dest_path):
    """Striimaa GET-vastauksen suoraan tiedostoon lukien pieni pala kerrallaan,
    jottei koko tiedostoa (main.py, kymmenia kt) tarvitse pitaa RAM:issa
    yhtena palana niin kuin http_get() tekisi. Vaatii Content-Length-otsakkeen
    (raw.githubusercontent.com lahettaa sen aina staattiselle tiedostolle)."""
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(OTA_HOST, 443, ssl=True), 10
    )
    try:
        req = (
            "GET %s HTTP/1.1\r\nHost: %s\r\nUser-Agent: picow-sahko/1.0\r\n"
            "Connection: close\r\n\r\n"
        ) % (path, OTA_HOST)
        writer.write(req.encode())
        await writer.drain()

        status_line = await asyncio.wait_for(reader.readline(), 10)
        parts = status_line.split(b" ", 2)
        code = int(parts[1]) if len(parts) > 1 else 0

        content_length = None
        while True:
            line = await asyncio.wait_for(reader.readline(), 10)
            if line in (b"\r\n", b"\n", b""):
                break
            if line.lower().startswith(b"content-length:"):
                content_length = int(line.split(b":", 1)[1].strip())

        if code != 200:
            raise OSError("HTTP-virhe %d" % code)
        if not content_length:
            raise OSError("palvelin ei ilmoittanut sisallon pituutta")

        written = 0
        with open(dest_path, "wb") as f:
            while written < content_length:
                chunk = await asyncio.wait_for(
                    reader.read(min(1024, content_length - written)), 10
                )
                if not chunk:
                    raise OSError("yhteys katkesi kesken latauksen")
                f.write(chunk)
                written += len(chunk)
        return written
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


def _ota_looks_valid(path, min_bytes=5000):
    """Kevyt jarkevyystarkistus ennen kayttoonottoa: riittavan iso tiedosto
    eika ilmiselvaa syntaksivirhetta. Ei voi taata etta koodi on virheeton,
    mutta nappaa yleisimman tavan miten paivitys menisi pieleen."""
    try:
        size = os.stat(path)[6]
    except Exception as e:
        return False, "tiedostoa ei loydy: %s" % e
    if size < min_bytes:
        return False, "tiedosto on epailyttavan pieni (%d tavua)" % size
    try:
        with open(path) as f:
            compile(f.read(), path, "exec")
    except SyntaxError as e:
        return False, "syntaksivirhe ladatussa tiedostossa: %s" % e
    except Exception:
        pass  # compile() ei ehka ole tuettu tassa builtissa - ohitetaan tarkistus
    gc.collect()
    return True, ""


async def ota_check_and_apply(force=False):
    """Tarkistaa onko GitHub-repossa APP_VERSIONista poikkeava version.txt, ja
    jos on (tai force=True), lataa main.py:n, tarkistaa sen jarkevyyden, ottaa
    varmuuskopion nykyisesta main.py:sta ja kaynnistaa laitteen uudelleen."""
    ota_status["checking"] = True
    ota_status["message"] = "Tarkistetaan paivitysta..."
    try:
        remote_version = await ota_check_version()
        if remote_version == APP_VERSION and not force:
            ota_status["message"] = "Jo uusin versio (%s)" % APP_VERSION
            return

        ota_status["message"] = "Ladataan versiota %s..." % remote_version
        size = await ota_download_to_file(OTA_PATH_PREFIX + "/main.py", OTA_STAGING_PATH)

        ok, err = _ota_looks_valid(OTA_STAGING_PATH)
        if not ok:
            os.remove(OTA_STAGING_PATH)
            ota_status["message"] = "Paivitys hylattiin: " + err
            return

        try:
            os.remove(OTA_BACKUP_PATH)
        except Exception:
            pass
        os.rename(OTA_MAIN_PATH, OTA_BACKUP_PATH)
        os.rename(OTA_STAGING_PATH, OTA_MAIN_PATH)

        ota_status["message"] = "Paivitetty (%d tavua, versio %s) - kaynnistetaan uudelleen" % (size, remote_version)
        print(ota_status["message"])
        await asyncio.sleep(1)  # antaa HTTP-vastauksen ehtia lahtea selaimelle
        machine.reset()
    except Exception as e:
        ota_status["message"] = "Paivitys epaonnistui: %s" % e
        print(ota_status["message"])
    finally:
        ota_status["checking"] = False


# =============================================================================
# WEB-KAYTTOLIITTYMA (staattinen HTML+CSS+JS, data haetaan /api/state:sta)
# =============================================================================

def _temps_row_html():
    """Rakentaa sivun ylalaidan lampotilaelementit BLE_ROOM_ORDER-jarjestyksessa,
    yksi <div class="temp-item"> per huone. id="temp-<huoneavain>" - JS taulukoi
    nama automaattisesti /api/state:n temps-kentan perusteella (updateTemps())."""
    parts = []
    for room in BLE_ROOM_ORDER:
        parts.append(
            '<div class="temp-item"><span class="temp-label">' + BLE_ROOM_LABELS[room]
            + '</span><span class="temp-value" id="temp-' + room + '">-</span></div>'
        )
    return "".join(parts)


_INDEX_HTML_TEMPLATE = '''<!doctype html>
<html lang="fi">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="theme-color" content="#0b0f17">
<title>Sahko ja kodinohjaus</title>
<style>
:root{
  --bg:#0b0f17; --card:#131a26; --card2:#1a2333; --line:#232e42;
  --text:#e7edf6; --dim:#8593a8; --accent:#f2b84b; --accent-ink:#221a05;
  --green:#4ad998; --red:#f2555c;
  --mono: ui-monospace, "SFMono-Regular", "Cascadia Mono", Consolas, monospace;
  --sans: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
  color-scheme: dark;
}
*{box-sizing:border-box;}
html,body{margin:0;background:var(--bg);color:var(--text);font-family:var(--sans);}
body{padding-bottom:2.5rem;}
h2{font-weight:600;margin:0 0 .6em;}
:focus-visible{outline:2px solid var(--accent);outline-offset:2px;}
.sr-only{position:absolute;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap;border:0;}

.topbar{
  position:sticky;top:0;z-index:10;display:flex;flex-direction:column;gap:.4rem;
  padding:.7rem 1rem .6rem;background:rgba(11,15,23,.92);backdrop-filter:blur(6px);
  border-bottom:1px solid var(--line);
}
.topbar-row{display:flex;align-items:center;justify-content:space-between;gap:1rem;}
.clock{font-family:var(--mono);font-size:1.3rem;letter-spacing:.04em;font-variant-numeric:tabular-nums;}
.price-now{display:flex;align-items:baseline;gap:.35em;}
.price-now-value{font-family:var(--mono);font-size:1.6rem;font-weight:600;color:var(--accent);font-variant-numeric:tabular-nums;}
.price-now-unit{color:var(--dim);font-size:.8rem;}
.status{width:10px;height:10px;border-radius:50%;background:var(--dim);flex:none;}
.status-ok{background:var(--green);}
.status-warn{background:var(--accent);}
.status-bad{background:var(--red);}
.temps-row{display:flex;align-items:baseline;gap:1.2rem;flex-wrap:wrap;}
.temp-item{display:flex;align-items:baseline;gap:.4em;font-size:.85rem;}
.temp-label{color:var(--dim);}
.temp-value{font-family:var(--mono);font-weight:600;color:var(--text);font-variant-numeric:tabular-nums;}
.temp-value.stale{color:var(--dim);font-weight:400;}

main{max-width:720px;margin:0 auto;padding:1rem;display:flex;flex-direction:column;gap:1rem;}
.card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:1rem 1.1rem;}
.card h2{font-size:.9rem;text-transform:uppercase;letter-spacing:.08em;color:var(--dim);}
.muted{color:var(--dim);font-size:.85rem;}

.bars{display:flex;align-items:flex-end;gap:4px;height:150px;overflow-x:auto;padding:0 0 1.6em;margin-top:.5rem;}
.bar-col{display:flex;flex-direction:column;align-items:center;justify-content:flex-end;min-width:30px;flex:1 0 30px;height:100%;position:relative;}
.bar-value{font-family:var(--mono);font-size:10px;color:var(--dim);margin-bottom:3px;white-space:nowrap;}
.bar{width:100%;border-radius:4px 4px 1px 1px;min-height:3px;}
.bar-hour{position:absolute;bottom:-1.5em;font-size:10px;color:var(--dim);font-family:var(--mono);}
.bar-col.current .bar{box-shadow:0 0 0 2px var(--accent);}
.bar-col.current .bar-hour{color:var(--accent);font-weight:700;}

.pin-card{border-top:1px solid var(--line);padding:.9rem 0;}
.pin-card:first-child{border-top:none;padding-top:.2rem;}
.pin-head{display:flex;align-items:center;justify-content:space-between;gap:.8rem;}
.pin-name{font-weight:600;}
.pin-status{color:var(--dim);font-size:.85rem;}
.pin-auto-badge{font-size:.78rem;color:var(--accent);min-height:1.2em;margin-top:.2rem;}

.switch{position:relative;display:inline-block;width:46px;height:26px;flex:none;}
.switch input{position:absolute;inset:0;opacity:0;margin:0;cursor:pointer;}
.slider{position:absolute;inset:0;background:#2a3446;border-radius:999px;transition:background .15s;pointer-events:none;}
.slider:before{content:"";position:absolute;height:20px;width:20px;left:3px;top:3px;background:#cfd8e3;border-radius:50%;transition:transform .15s;}
.switch input:checked + .slider{background:var(--accent);}
.switch input:checked + .slider:before{transform:translateX(20px);background:var(--accent-ink);}
.switch input:focus-visible + .slider{outline:2px solid var(--accent);outline-offset:2px;}

details{margin-top:.6rem;}
summary{cursor:pointer;color:var(--dim);font-size:.85rem;user-select:none;}
.setting-block{background:var(--card2);border-radius:10px;padding:.7rem .8rem;margin-top:.6rem;}
.setting-block label{display:flex;align-items:center;gap:.5em;font-size:.9rem;}
.setting-block .row{display:flex;align-items:center;gap:.5em;margin:.5em 0;flex-wrap:wrap;}
input[type=time],input[type=number]{
  background:var(--bg);border:1px solid var(--line);color:var(--text);
  border-radius:8px;padding:.35em .5em;font-family:var(--mono);width:6.5em;
}
input[type=checkbox]{width:16px;height:16px;accent-color:var(--accent);}
.setting-block button{
  background:var(--accent);color:var(--accent-ink);border:none;border-radius:8px;
  padding:.45em .9em;font-weight:600;cursor:pointer;margin-top:.3em;font:inherit;
}
.setting-block button:active{transform:translateY(1px);}

.toast{
  position:fixed;left:50%;bottom:1.2rem;transform:translate(-50%,10px);
  background:var(--green);color:#06281a;padding:.5em 1em;border-radius:999px;
  font-size:.85rem;font-weight:600;opacity:0;transition:opacity .2s, transform .2s;pointer-events:none;
}
.toast.show{opacity:1;transform:translate(-50%,0);}

.foot{text-align:center;color:var(--dim);font-size:.75rem;padding:1rem 0 .5rem;font-family:var(--mono);}

@media (prefers-reduced-motion: reduce){ *{transition:none !important;} }
</style>
</head>
<body>
<h1 class="sr-only">Sahko ja kodinohjaus</h1>
<header class="topbar">
  <div class="topbar-row">
    <div class="clock" id="clock">--:--:--</div>
    <div class="price-now"><span class="price-now-value" id="priceNow">-</span><span class="price-now-unit">snt/kWh</span></div>
    <div class="status" id="statusDot" title="Yhteystila"></div>
  </div>
  <div class="temps-row">%%TEMPS_ROW%%<div class="temp-item"><span class="temp-label">Kulutus</span><span class="temp-value" id="shelly-power">-</span></div></div>
</header>
<main>
  <section class="card">
    <h2>Taman vuorokauden hinnat</h2>
    <div class="bars" id="chartToday"></div>
  </section>
  <section class="card">
    <h2>Seuraavan vuorokauden hinnat</h2>
    <div class="bars" id="chartTomorrow"></div>
    <p class="muted" id="tomorrowMsg" hidden>Huomisen hinnat eivat ole viela saatavilla.</p>
  </section>
  <section class="card">
    <h2>Ohjaukset</h2>
    <div id="pins"></div>
  </section>
  <section class="card">
    <h2>Ohjelmistopaivitys</h2>
    <div class="setting-block">
      <div>Kaynnissa oleva versio: <span id="appVersion">-</span></div>
      <button onclick="checkOta()">Tarkista paivitys</button>
      <div class="muted" id="otaMsg" style="margin-top:.5em;"></div>
    </div>
  </section>
</main>
<footer class="foot">Osoite: <span id="ipAddr">-</span></footer>
<div class="toast" id="toast"></div>
<script>
let pinsRendered = false;
let clockBaseMs = 0, clockBaseStr = "00:00:00";

function pad(n){ return n < 10 ? "0"+n : ""+n; }

function tickClock(){
  if(!clockBaseMs) return;
  const parts = clockBaseStr.split(":").map(Number);
  let total = parts[0]*3600 + parts[1]*60 + parts[2];
  total += Math.floor((Date.now()-clockBaseMs)/1000);
  total = ((total % 86400) + 86400) % 86400;
  const h = Math.floor(total/3600), m = Math.floor((total%3600)/60), s = total%60;
  document.getElementById("clock").textContent = pad(h)+":"+pad(m)+":"+pad(s);
}

const PRICE_COLOR_MAX = 20; // snt/kWh - varin asteikko on kiintea, ei riipu vuorokauden min/maxista

function priceColor(t){
  t = Math.max(0, Math.min(1, t));
  let c1, c2, f;
  if(t < 0.5){ c1=[74,217,152]; c2=[242,184,75]; f=t/0.5; }
  else { c1=[242,184,75]; c2=[242,85,92]; f=(t-0.5)/0.5; }
  const r = Math.round(c1[0]+(c2[0]-c1[0])*f);
  const g = Math.round(c1[1]+(c2[1]-c1[1])*f);
  const b = Math.round(c1[2]+(c2[2]-c1[2])*f);
  return "rgb("+r+","+g+","+b+")";
}

function renderChart(elId, arr, currentHour){
  const el = document.getElementById(elId);
  if(!arr || !arr.length){ el.innerHTML = ""; return; }
  const prices = arr.map(x => x.p);
  const min = Math.min.apply(null, prices), max = Math.max.apply(null, prices);
  const span = (max - min) || 1;
  el.innerHTML = arr.map(function(x){
    const rel = (x.p - min) / span;
    const h = 12 + rel*88;
    const cur = (x.h === currentHour) ? " current" : "";
    return '<div class="bar-col'+cur+'">'
      + '<span class="bar-value">'+x.p.toFixed(2)+'</span>'
      + '<div class="bar" style="height:'+h.toFixed(0)+'%;background:'+priceColor(x.p/PRICE_COLOR_MAX)+'"></div>'
      + '<span class="bar-hour">'+pad(x.h)+'</span>'
      + '</div>';
  }).join("");
}

function autoModeLabel(m){
  return m === "temp" ? "Lampotila" : m === "price" ? "Hinta" : m === "sched" ? "Ajastus" : "";
}

function renderPinsOnce(s){
  const el = document.getElementById("pins");
  el.innerHTML = Object.keys(s.pins).map(function(id){
    const p = s.pins[id];
    const tempBlock = !p.temp_capable ? "" : (
      '<div class="setting-block">'
      + '<label><input type="checkbox" id="temp-en-'+id+'" '+(p.temp_enabled?"checked":"")+'> Lampotilaohjaus</label>'
      + '<div class="row"><span>Tavoite</span>'
      + '<input type="number" step="0.5" id="temp-target-'+id+'" value="'+p.temp_target+'" aria-label="Tavoitelampotila">'
      + '<span>&deg;C</span></div>'
      + '<div class="muted" id="temp-reading-'+id+'">Mitattu lampotila: -</div>'
      + '<button onclick="saveTemp('+id+')">Tallenna</button>'
      + '</div>'
    );
    return (
      '<div class="pin-card">'
      + '<div class="pin-head"><div>'
      + '<div class="pin-name">'+p.name+'</div>'
      + '<div class="pin-status" id="status-'+id+'">'+(p.state?"Paalla":"Pois")+'</div>'
      + '</div>'
      + '<label class="switch"><input type="checkbox" id="toggle-'+id+'" '+(p.state?"checked":"")
      + ' onchange="togglePin('+id+',this.checked)"><span class="slider"></span></label>'
      + '</div>'
      + '<div class="pin-auto-badge" id="auto-'+id+'"></div>'
      + '<details><summary>Asetukset</summary>'
      + '<div class="setting-block">'
      + '<label><input type="checkbox" id="sched-en-'+id+'" '+(p.sched_enabled?"checked":"")+'> Ajastus</label>'
      + '<div class="row">'
      + '<input type="time" id="sched-on-'+id+'" value="'+p.sched_on+'" aria-label="Ajastuksen alkuaika">'
      + '<span>-</span>'
      + '<input type="time" id="sched-off-'+id+'" value="'+p.sched_off+'" aria-label="Ajastuksen loppuaika">'
      + '</div>'
      + '<button onclick="saveSchedule('+id+')">Tallenna</button>'
      + '</div>'
      + '<div class="setting-block">'
      + '<label><input type="checkbox" id="price-en-'+id+'" '+(p.price_enabled?"checked":"")+'> Hintaohjaus</label>'
      + '<div class="muted">Kytkeytyy paalle, kun hinta on rajan alla tai yhta suuri</div>'
      + '<div class="row">'
      + '<input type="number" step="0.1" id="price-limit-'+id+'" value="'+p.price_limit+'" aria-label="Hinnan ylaraja">'
      + '<span>snt/kWh</span>'
      + '</div>'
      + '<button onclick="savePrice('+id+')">Tallenna</button>'
      + '</div>'
      + tempBlock
      + '</details>'
      + '</div>'
    );
  }).join("");
}

function updatePinsLive(s){
  Object.keys(s.pins).forEach(function(id){
    const p = s.pins[id];
    const t = document.getElementById("toggle-"+id);
    if(t) t.checked = p.state;
    const st = document.getElementById("status-"+id);
    if(st) st.textContent = p.state ? "Paalla" : "Pois";
    const auto = document.getElementById("auto-"+id);
    if(auto) auto.textContent = p.auto_mode ? ("Automaatio: "+autoModeLabel(p.auto_mode)) : "";
    if(p.temp_capable){
      const tr = document.getElementById("temp-reading-"+id);
      if(tr){
        tr.textContent = "Mitattu lampotila: "
          + (p.temp_value != null ? p.temp_value.toFixed(1)+" \u00b0C" : "ei tietoa")
          + (p.temp_stale ? " (vanhentunut)" : "");
      }
    }
  });
}

async function togglePin(pin, on){
  try{
    await fetch("/api/pin", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({pin:+pin, on:on})});
  }catch(e){}
  fetchState();
}
async function postJSON(url, obj){
  try{
    const res = await fetch(url, {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(obj)});
    if(res.ok) showToast("Tallennettu");
  }catch(e){}
  fetchState();
}
function saveSchedule(pin){
  postJSON("/api/schedule", {
    pin: +pin,
    enabled: document.getElementById("sched-en-"+pin).checked,
    on: document.getElementById("sched-on-"+pin).value,
    off: document.getElementById("sched-off-"+pin).value
  });
}
function savePrice(pin){
  postJSON("/api/price", {
    pin: +pin,
    enabled: document.getElementById("price-en-"+pin).checked,
    limit: parseFloat(document.getElementById("price-limit-"+pin).value)
  });
}
function saveTemp(pin){
  postJSON("/api/temp", {
    pin: +pin,
    enabled: document.getElementById("temp-en-"+pin).checked,
    target: parseFloat(document.getElementById("temp-target-"+pin).value)
  });
}

function checkOta(){
  document.getElementById("otaMsg").textContent = "Kaynnistetaan tarkistusta...";
  fetch("/api/ota", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({force:false})})
    .catch(function(){})
    .then(function(){ fetchState(); });
}

let toastTimer = null;
function showToast(msg){
  const t = document.getElementById("toast");
  t.textContent = msg; t.classList.add("show");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(function(){ t.classList.remove("show"); }, 1600);
}

function setStatus(reachable, wifi, ble){
  const dot = document.getElementById("statusDot");
  if(!reachable){ dot.className = "status status-bad"; dot.title = "Ei yhteytta laitteeseen"; return; }
  if(wifi && ble){ dot.className = "status status-ok"; dot.title = "WiFi yhteydessa, BLE-skannaus kaynnissa"; }
  else if(wifi){ dot.className = "status status-warn"; dot.title = "WiFi yhteydessa, BLE-skannaus ei kaynnissa"; }
  else { dot.className = "status status-bad"; dot.title = "Ei WiFi-yhteytta"; }
}

function updateTempItem(elId, t, unit){
  const el = document.getElementById(elId);
  if(!el) return;
  if(!t || t.value == null){
    el.textContent = "-";
    el.classList.remove("stale");
    el.title = "";
    return;
  }
  el.textContent = t.value.toFixed(1) + (unit || " \u00b0C");
  el.classList.toggle("stale", !!t.stale);
  el.title = t.stale ? "Mittaus vanhentunut" : "";
}

function updateTemps(temps){
  if(!temps) return;
  Object.keys(temps).forEach(function(room){
    updateTempItem("temp-"+room, temps[room]);
  });
}

async function fetchState(){
  try{
    const res = await fetch("/api/state");
    if(!res.ok) throw new Error("http "+res.status);
    const s = await res.json();

    clockBaseMs = Date.now();
    clockBaseStr = s.time;
    tickClock();

    document.getElementById("priceNow").textContent = s.price_now != null ? s.price_now.toFixed(2) : "-";
    document.getElementById("ipAddr").textContent = s.ip || "-";
    document.getElementById("appVersion").textContent = s.app_version || "-";
    document.getElementById("otaMsg").textContent = s.ota_message || "";
    updateTemps(s.temps);
    updateTempItem("shelly-power", s.shelly_power, " kW");

    renderChart("chartToday", s.today, s.current_hour);
    const tBars = document.getElementById("chartTomorrow");
    const tMsg = document.getElementById("tomorrowMsg");
    if(s.tomorrow && s.tomorrow.length){ tBars.hidden = false; tMsg.hidden = true; renderChart("chartTomorrow", s.tomorrow, -1); }
    else { tBars.hidden = true; tMsg.hidden = false; }

    if(!pinsRendered){ renderPinsOnce(s); pinsRendered = true; }
    updatePinsLive(s);

    setStatus(true, s.wifi, s.ble);
  }catch(e){
    setStatus(false);
  }
}

fetchState();
setInterval(fetchState, 15000);
setInterval(tickClock, 1000);
</script>
</body>
</html>
'''

INDEX_HTML = _INDEX_HTML_TEMPLATE.replace("%%TEMPS_ROW%%", _temps_row_html()).encode()


async def send_response(writer, code, ctype, body_bytes):
    reason = {200: "OK", 400: "Bad Request", 404: "Not Found",
              413: "Payload Too Large", 500: "Internal Server Error"}.get(code, "")
    header = (
        "HTTP/1.1 %d %s\r\nContent-Type: %s\r\nContent-Length: %d\r\n"
        "Connection: close\r\n\r\n" % (code, reason, ctype, len(body_bytes))
    )
    writer.write(header.encode())
    writer.write(body_bytes)
    await writer.drain()


def build_state():
    update_price_now()
    lt = local_now()
    pins_out = {}
    for p in PIN_NUMBERS:
        c = pins[p]
        entry = {
            "name": c["name"],
            "state": c["state"],
            "sched_enabled": c["sched_enabled"],
            "sched_on": c["sched_on"],
            "sched_off": c["sched_off"],
            "price_enabled": c["price_enabled"],
            "price_limit": c["price_limit"],
            "temp_capable": c["temp_capable"],
            "auto_mode": c.get("auto_mode"),
        }
        if c["temp_capable"]:
            entry["temp_enabled"] = c["temp_enabled"]
            entry["temp_target"] = c["temp_target"]
            val, stale = get_temp(TEMP_ROOM_FOR_PIN[p])
            entry["temp_value"] = val
            entry["temp_stale"] = stale
        pins_out[str(p)] = entry

    temps_out = {}
    for room in BLE_ROOM_ORDER:
        val, stale = get_temp(room)
        temps_out[room] = {"value": val, "stale": stale}

    shelly_val, shelly_stale = get_shelly_power()

    return {
        "time": "%02d:%02d:%02d" % (lt[3], lt[4], lt[5]),
        "date": "%02d.%02d.%04d" % (lt[2], lt[1], lt[0]),
        "current_hour": lt[3],
        "price_now": price_now,
        "today": [{"h": h, "p": pr} for h, pr in prices_today],
        "tomorrow": [{"h": h, "p": pr} for h, pr in prices_tomorrow],
        "prices_valid": prices_valid,
        "temps": temps_out,
        "shelly_power": {"value": shelly_val, "stale": shelly_stale},
        "wifi": wifi_status.get("connected", False),
        "ble": ble_status.get("active", False),
        "ntp": ntp_synced,
        "ip": wlan.ifconfig()[0] if (wlan and wifi_status.get("connected")) else None,
        "pins": pins_out,
        "app_version": APP_VERSION,
        "ota_checking": ota_status["checking"],
        "ota_message": ota_status["message"],
    }


async def handle_api_pin(writer, body):
    try:
        d = json.loads(body)
        p = int(d["pin"])
        if p not in PIN_NUMBERS:
            raise ValueError("tuntematon pinni")
        set_pin(p, bool(d["on"]))
        await send_response(writer, 200, "application/json", b'{"ok":true}')
    except Exception:
        await send_response(writer, 400, "application/json", b'{"ok":false}')


async def handle_api_schedule(writer, body):
    try:
        d = json.loads(body)
        p = int(d["pin"])
        if p not in PIN_NUMBERS:
            raise ValueError("tuntematon pinni")
        pins[p]["sched_enabled"] = bool(d.get("enabled", False))
        pins[p]["sched_on"] = str(d.get("on", pins[p]["sched_on"]))[:5]
        pins[p]["sched_off"] = str(d.get("off", pins[p]["sched_off"]))[:5]
        save_settings()
        await send_response(writer, 200, "application/json", b'{"ok":true}')
    except Exception:
        await send_response(writer, 400, "application/json", b'{"ok":false}')


async def handle_api_price(writer, body):
    try:
        d = json.loads(body)
        p = int(d["pin"])
        if p not in PIN_NUMBERS:
            raise ValueError("tuntematon pinni")
        pins[p]["price_enabled"] = bool(d.get("enabled", False))
        pins[p]["price_limit"] = round(float(d.get("limit", pins[p]["price_limit"])), 2)
        save_settings()
        await send_response(writer, 200, "application/json", b'{"ok":true}')
    except Exception:
        await send_response(writer, 400, "application/json", b'{"ok":false}')


async def handle_api_temp(writer, body):
    try:
        d = json.loads(body)
        p = int(d["pin"])
        if p not in TEMP_ROOM_FOR_PIN:
            raise ValueError("pinnilla ei ole lampotilaohjausta")
        pins[p]["temp_enabled"] = bool(d.get("enabled", False))
        pins[p]["temp_target"] = round(float(d.get("target", pins[p]["temp_target"])), 1)
        save_settings()
        await send_response(writer, 200, "application/json", b'{"ok":true}')
    except Exception:
        await send_response(writer, 400, "application/json", b'{"ok":false}')


async def handle_api_ota(writer, body):
    if ota_status["checking"]:
        await send_response(writer, 409, "application/json", b'{"ok":false,"error":"jo kaynnissa"}')
        return
    try:
        d = json.loads(body) if body else {}
        force = bool(d.get("force", False))
    except Exception:
        force = False
    # Kaynnistetaan taustatehtavana, jotta HTTP-vastaus ehtii lahtea
    # selaimelle ennen kuin lataus/uudelleenkaynnistys mahdollisesti alkaa.
    asyncio.create_task(ota_check_and_apply(force=force))
    await send_response(writer, 200, "application/json", b'{"ok":true}')


async def handle_client(reader, writer):
    try:
        request_line = await asyncio.wait_for(reader.readline(), 5)
        if not request_line:
            return
        method, path, _ver = request_line.decode().split(" ", 2)

        content_length = 0
        while True:
            line = await asyncio.wait_for(reader.readline(), 5)
            if line in (b"\r\n", b"\n", b""):
                break
            low = line.lower()
            if low.startswith(b"content-length:"):
                try:
                    content_length = int(line.split(b":", 1)[1].strip())
                except Exception:
                    content_length = 0

        body = b""
        if content_length:
            if content_length > 2048:
                await send_response(writer, 413, "text/plain", b"Liian iso pyynto")
                return
            body = await asyncio.wait_for(reader.readexactly(content_length), 5)

        path_only = path.split("?", 1)[0]

        if method == "GET" and path_only == "/":
            gc.collect()
            await send_response(writer, 200, "text/html; charset=utf-8", INDEX_HTML)
        elif method == "GET" and path_only == "/api/state":
            gc.collect()
            await send_response(writer, 200, "application/json", json.dumps(build_state()).encode())
        elif method == "POST" and path_only == "/api/pin":
            await handle_api_pin(writer, body)
        elif method == "POST" and path_only == "/api/schedule":
            await handle_api_schedule(writer, body)
        elif method == "POST" and path_only == "/api/price":
            await handle_api_price(writer, body)
        elif method == "POST" and path_only == "/api/temp":
            await handle_api_temp(writer, body)
        elif method == "POST" and path_only == "/api/ota":
            await handle_api_ota(writer, body)
        else:
            await send_response(writer, 404, "text/plain", b"Ei loydy")
    except Exception as e:
        print("Web-pyynnon kasittely epaonnistui:", e)
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
        gc.collect()


# =============================================================================
# WIFI JA PAAOHJELMA
# =============================================================================

async def wifi_connect():
    global wlan
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    attempt = 0
    while not wlan.isconnected():
        attempt += 1
        print("WiFi-yhteysyritys", attempt, "-", WIFI_SSID)
        try:
            wlan.connect(WIFI_SSID, WIFI_PASSWORD)
        except Exception as e:
            print("wlan.connect() epaonnistui:", e)
        t0 = time.ticks_ms()
        while not wlan.isconnected() and time.ticks_diff(time.ticks_ms(), t0) < 20000:
            await asyncio.sleep_ms(250)
        if not wlan.isconnected():
            print("Ei viela yhteytta, yritetaan uudelleen...")
            await asyncio.sleep(5)
    wifi_status["connected"] = True
    print("WiFi yhdistetty, IP:", wlan.ifconfig()[0])


async def wifi_watchdog_task():
    """Pitaa wifi_status-tiedon ajan tasalla ja yhdistaa WiFin uudelleen jos se
    katkeaa. Aiemmin tasta huolehti osittain mqtt_as-kirjaston sisainen WiFi-
    valvonta; nyt kun lampotilat luetaan BLE:lla eika MQTT:lla, tama hoitaa saman."""
    while True:
        await asyncio.sleep(10)
        if wlan is None:
            continue
        if wlan.isconnected():
            wifi_status["connected"] = True
        else:
            wifi_status["connected"] = False
            print("WiFi-yhteys poikki, yritetaan yhdistaa uudelleen...")
            try:
                await wifi_connect()
            except Exception as e:
                print("WiFi-uudelleenyhdistys epaonnistui:", e)


async def main():
    print("=== Kaynnistetaan sahko- ja kodinohjausjarjestelmaa ===")
    load_settings()
    await wifi_connect()

    asyncio.create_task(ble_scan_task())
    asyncio.create_task(wifi_watchdog_task())
    asyncio.create_task(ntp_sync_task())
    asyncio.create_task(price_update_task())
    asyncio.create_task(shelly_power_task())
    asyncio.create_task(automation_task())

    await asyncio.start_server(handle_client, "0.0.0.0", HTTP_PORT)
    print("Web-palvelin kaynnissa, portti", HTTP_PORT)

    while True:
        await asyncio.sleep(60)
        gc.collect()
        print("Vapaata muistia gc.collect() jalkeen:", gc.mem_free(), "tavua")


try:
    asyncio.run(main())
except KeyboardInterrupt:
    pass
finally:
    try:
        asyncio.new_event_loop()
    except Exception:
        pass