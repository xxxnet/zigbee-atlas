import datetime as dt
from functools import partial
from http.server import ThreadingHTTPServer
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch
from urllib.request import Request,urlopen
from urllib.error import HTTPError
from zoneinfo import ZoneInfo

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"zigbee_atlas/app"))
from app import DEFAULTS,Handler,SetupError,Worker,atomic_publish,find_source,source_metadata
from log_reader import parse_line


class AppTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.source=self.root/"input";self.source.mkdir()
        self.source.joinpath("configuration.yaml").write_text("devices:\n  '0x0000000000000002':\n    friendly_name: demo-door\nserial:\n  adapter: ember\n")
        devices=[{"id":1,"ieeeAddr":"0x0000000000000000","nwkAddr":0,"type":"Coordinator"},
                 {"id":2,"ieeeAddr":"0x0000000000000001","nwkAddr":10,"type":"Router"},
                 {"id":3,"ieeeAddr":"0x0000000000000002","nwkAddr":20,"type":"EndDevice"}]
        self.source.joinpath("database.db").write_text("\n".join(json.dumps(x) for x in devices)+"\n")

    def tearDown(self):self.temp.cleanup()

    def test_auto_finds_source_without_machine_specific_path(self):
        self.assertEqual(find_source("auto",[self.root]),self.source)

    def test_ambiguity_requires_explicit_selection(self):
        other=self.root/"other";other.mkdir()
        for p in self.source.iterdir():other.joinpath(p.name).write_bytes(p.read_bytes())
        with self.assertRaises(SetupError):find_source("auto",[self.root])

    def test_external_devices_yaml(self):
        self.source.joinpath("configuration.yaml").write_text("devices: devices.yaml\nserial:\n  adapter: ember\n")
        self.source.joinpath("devices.yaml").write_text("'0x0000000000000002':\n  friendly_name: external-door\n")
        rows,adapter=source_metadata(self.source)
        self.assertEqual(next(d for d in rows if d["id"].endswith("2"))["name"],"external-door")
        self.assertEqual(adapter,"ember")

    def test_custom_mqtt_prefix_and_timezone(self):
        line="[2026-10-23 12:00:00] info: MQTT publish: topic 'house/zigbee/demo-door/availability', payload '{\"state\":\"offline\"}'"
        with patch.dict(os.environ,{"ATLAS_MQTT_BASE_TOPIC":"house/zigbee","ATLAS_TIMEZONE":"UTC"}):
            stamp,event=parse_line(line)
        self.assertEqual(event["name"],"demo-door")
        self.assertEqual(stamp,int(dt.datetime(2026,10,23,12,tzinfo=dt.timezone.utc).timestamp()))

    def test_cycle_renders_without_changing_source_files(self):
        logs=self.source/"log";logs.mkdir()
        stamp=dt.datetime.now(ZoneInfo("Europe/Prague")).strftime("%Y-%m-%d %H:%M:%S")
        logs.joinpath("log.log").write_text(f"[{stamp}] debug: ezspIncomingRouteRecordHandler: source=20 sourceEui=0x0000000000000002 lastHopLqi=180 lastHopRssi=-50 relayCount=1 relayList=10\n")
        before={str(p.relative_to(self.source)):hashlib.sha256(p.read_bytes()).hexdigest() for p in self.source.rglob("*") if p.is_file()}
        data=self.root/"private";data.mkdir()
        worker=Worker({**DEFAULTS,"zigbee2mqtt_directory":str(self.source)},data)
        worker.cycle();worker.cycle()
        after={str(p.relative_to(self.source)):hashlib.sha256(p.read_bytes()).hexdigest() for p in self.source.rglob("*") if p.is_file()}
        self.assertEqual(before,after)
        self.assertTrue((data/"reader-checkpoint.json").is_file())
        self.assertTrue((worker.site/"manifest.js").is_file())
        self.assertEqual(worker.status["state"],"ready")

    def test_ingress_rejects_untrusted_peer_for_get_and_head(self):
        data=self.root/"private";data.mkdir()
        worker=Worker(DEFAULTS,data)
        server=ThreadingHTTPServer(("127.0.0.1",0),partial(Handler,worker=worker,ingress=True))
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            for method in ("GET","HEAD"):
                with self.assertRaises(HTTPError) as error:urlopen(Request(f"http://127.0.0.1:{server.server_port}/",method=method),timeout=5)
                self.assertEqual(error.exception.code,403)
        finally:server.shutdown();server.server_close();thread.join()


if __name__=="__main__":unittest.main()
