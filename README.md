# Zigbee Atlas

Malý pasivní atlas Zigbee2MQTT: historie sítě, stálé pozice zařízení a přehrávač snímků.

Vznikl při hledání náhodných výpadků Zigbee sítě. Čte existující logy a ukazuje pozorované cesty, připojení, delší ticho a návraty zařízení. Do Zigbee neposílá žádné příkazy.

## Instalace v Home Assistantu

1. **Nastavení → Aplikace → Obchod s aplikacemi → ⋮ → Repozitáře** (ve starších verzích „Doplňky“).
2. Přidej `https://github.com/xxxnet/zigbee-atlas`.
3. Nainstaluj **Zigbee Atlas**, zapni **Spouštět při startu** a spusť jej.
4. Otevři webové rozhraní. Datovou složku Zigbee2MQTT zkusí najít automaticky.

První instalace sestavuje kontejner přímo v HA a může několik minut trvat. Proxmox, SSH ani heslo k MQTT nejsou potřeba. Podporované architektury obalu jsou amd64 a aarch64; první verze má české rozhraní.

### Co musí běžet

- Home Assistant OS / instalace se Supervisorem a Zigbee2MQTT.
- Z2M zapisuje debug log do souboru:

```yaml
advanced:
  log_level: debug
  log_output:
    - console
    - file
```

Přidej tyto volby do existující části `advanced`, nevytvářej ji podruhé. Změna může vyžadovat restart Zigbee2MQTT. Atlas konfiguraci Z2M sám nemění.

**Důležité omezení první verze:** pasivní cesty a rodiče jsou parsované z logů adaptéru **Ember / EZSP / Silicon Labs**. Jiný koordinátor se Z2M může poskytnout příjmy a stavy zařízení, ale mapa může zůstat bez spojů. ZHA není podporované. Nezaměňovat chybějící spoj za výpadek. Před instalací na jiný adaptér je dobré nejdřív dodat anonymizovaný výřez jeho logu pro doplnění parseru.

## Nastavení

Výchozí konfigurace:

```yaml
zigbee2mqtt_directory: auto
log_directory: auto
timezone: Europe/Prague
mqtt_base_topic: zigbee2mqtt
poll_seconds: 60
render_minutes: 60
detail_days: 30
```

Pokud automatické hledání nenajde jedinou síť, nastav cestu viditelnou **z aplikace Atlas**, například `/homeassistant/zigbee2mqtt` nebo `/addon_configs/45df7312_zigbee2mqtt`. Cesty se mohou podle instalace lišit. V adresáři musí být `configuration.yaml` a `database.db`; konfigurace zařízení může být i v samostatném YAML souboru uvnitř této složky.

`log_directory: auto` hledá standardní složky `log`/`logs`, včetně podadresářů s časem spuštění. U vlastní cesty ji vyplň ručně. Při více sítích je nutné vybrat jednu.

## Přehrávač

- Snímky po dnech, hodinách nebo důležitých událostech.
- Interval přehrávání 0,25–10 sekund, smyčka, časový posuvník.
- Hledání zařízení, výběr jeho cesty, zoom a export SVG.
- Pozice podle IEEE adresy se při běžných aktualizacích nemění.
- Šipky se vyhýbají ostatním uzlům.

Syté spoje jsou poslední zachycené cesty. Šedé jsou jiné dříve pozorované cesty, nikoliv ověřené aktuálně funkční zálohy. Přerušovaně fialově se ukazuje rodič při připojení, pokud není novější celá cesta.

„Ticho“ znamená překročenou dobu bez příjmu při fungujícím sběru. Není to důkaz závady. Chybějící route record ani mezera v logu samy nevytvářejí offline stav. Přijatá zpráva zase nepotvrzuje správnou funkci měření nebo relé.

## Data

Atlas má připojené konfigurační složky jen pro čtení. Vlastní databázi, checkpointy a mapy ukládá do soukromého `/data` aplikace. Web se otevírá přes HA ingress; přímo se nevystavuje síťový port. Do map se nepřenášejí MQTT hesla ani síťový klíč.

Detailní události se drží podle `detail_days`, hotové snímky a denní SVG se automaticky nemažou. Uchovává se také poslední starší stav a přiřazení síťových adres. Při nedostatku místa se další sběr zastaví; poslední mapa zůstane dostupná.

Sběr potřebuje průběžný přístup k rotujícím logům. Co se stihne z logů smazat během delšího vypnutí Atlasu, už neobnoví. Na začátku tedy uvidíš jen dostupné starší logy a další historii si nasbírá sám. Součástí tohoto repozitáře není ničí domácí síť ani historie.

## Vývoj / vlastní Docker

```sh
python3 -m unittest discover -s tests -v
docker build -t zigbee-atlas:local zigbee_atlas
```

Pro vlastní Docker připoj adresář Z2M jen ke čtení, trvalý `/data` a nastav `ATLAS_INGRESS=false`. Volby jsou v `/data/options.json`. Port8099 si zpřístupni podle své sítě; bez ingressu Atlas sám přihlašování neposkytuje.

Příklad pouze na místním počítači:

```sh
docker run --rm -p 127.0.0.1:8099:8099 \
  -e ATLAS_INGRESS=false \
  -v zigbee-atlas-data:/data \
  -v /cesta/k/zigbee2mqtt:/homeassistant/zigbee2mqtt:ro \
  zigbee-atlas:local
```

První malá verze; neobsahuje cloud, aktivní skenování sítě ani automatické opravy zařízení. Základem obalu je [formát HA aplikací](https://contributors.home-assistant.io/apps/reference/configuration/) a [ingress](https://developers.home-assistant.io/docs/apps/presentation/).
