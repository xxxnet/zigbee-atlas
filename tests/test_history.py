import json
import datetime as dt
import os
from pathlib import Path
import sys
import tempfile
import unittest
from zoneinfo import ZoneInfo

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"zigbee_atlas/app"))
from core import History, Router, connect, ingest, initial_layout, intersects,maintain
from log_reader import parse_line, read_paths, metadata


class HistoryTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name)
        self.db=connect(self.root)
        self.t=1792700000
        self.devices=[{"id":"c","name":"Koordinátor","nwk":0,"role":"Coordinator"},
                      {"id":"r","name":"router","nwk":10,"role":"Router"},
                      {"id":"s","name":"sensor","nwk":20,"role":"EndDevice","manufacturer":"_TZE200_pay2byax"}]
        ingest(self.db,{"devices":self.devices})

    def tearDown(self):
        self.db.close();self.temp.cleanup()

    def add(self,events,coverage=None):
        ingest(self.db,{"events":events,"coverage":coverage or []})
        return History(self.db)

    def test_future_packets_do_not_change_past_frame(self):
        h=self.add([{"t":self.t,"k":"route","id":"s","nwk":20,"relays":[10]},
                    {"t":self.t+100,"k":"route","id":"s","nwk":20,"relays":[]}])
        self.assertEqual(h.frame(self.t+50)["paths"]["s"],["s","r","c"])
        self.assertEqual(h.frame(self.t+100)["paths"]["s"],["s","c"])

    def test_new_join_overrides_old_parent_without_inventing_full_route(self):
        h=self.add([{"t":self.t,"k":"route","id":"s","nwk":20,"relays":[10]},
                    {"t":self.t+10,"k":"join","id":"s","nwk":20,"parent_nwk":0,"status":"STANDARD_SECURITY_SECURED_REJOIN"}])
        frame=h.frame(self.t+20)
        self.assertEqual(frame["nodes"]["s"]["parent"],"c")
        self.assertTrue(frame["nodes"]["s"]["parent_confirmed"])
        self.assertEqual(next(e for e in frame["edges"] if e["id"]=="s>c")["kind"],"parent")
        self.assertEqual(next(e for e in frame["edges"] if e["id"]=="s>r")["kind"],"historical")

    def test_later_route_overrides_old_join_in_inspector(self):
        h=self.add([{"t":self.t,"k":"join","id":"s","nwk":20,"parent_nwk":10,"status":"REJOIN"},
                    {"t":self.t+10,"k":"route","id":"s","nwk":20,"relays":[]}])
        self.assertEqual(h.frame(self.t+20)["nodes"]["s"]["parent"],"c")

    def test_coverage_gap_does_not_mark_device_silent(self):
        h=self.add([{"t":self.t,"k":"seen","name":"sensor"}],
                   [{"start":self.t,"end":self.t+100,"scope":"all"},
                    {"start":self.t+10000,"end":self.t+12000,"scope":"all"}])
        self.assertEqual(h.frame(self.t+12000)["nodes"]["s"]["status"],"unobserved")
        self.assertFalse(any(e["type"]=="silent" for e in h.notifications))

    def test_silence_and_recovery_with_continuous_coverage(self):
        h=self.add([{"t":self.t,"k":"seen","name":"sensor"},
                    {"t":self.t+10000,"k":"seen","name":"sensor"}],
                   [{"start":self.t,"end":self.t+11000,"scope":"all"}])
        self.assertEqual(h.frame(self.t+8000)["nodes"]["s"]["status"],"silent")
        self.assertEqual(h.frame(self.t+10001)["nodes"]["s"]["status"],"observed")
        self.assertTrue(any(e["type"]=="recovery" for e in h.notifications))

    def test_filtered_coverage_applies_only_to_monitored_devices(self):
        h=self.add([{"t":self.t,"k":"seen","name":"sensor"},{"t":self.t,"k":"seen","name":"router"}],
                   [{"start":self.t,"end":self.t+10000,"scope":"selected","devices":["sensor"]}])
        f=h.frame(self.t+9000)
        self.assertEqual(f["nodes"]["s"]["status"],"silent")
        self.assertEqual(f["nodes"]["r"]["status"],"unobserved")

    def test_reused_nwk_resolves_at_event_time(self):
        self.devices.append({"id":"r2","name":"new-router","nwk":10,"role":"Router"})
        ingest(self.db,{"devices":self.devices})
        h=self.add([{"t":self.t,"k":"route","id":"r","nwk":10,"relays":[]},
                    {"t":self.t+1,"k":"route","id":"s","nwk":20,"relays":[10]},
                    {"t":self.t+20,"k":"join","id":"r2","nwk":10,"parent_nwk":0,"status":"JOIN"},
                    {"t":self.t+21,"k":"route","id":"s","nwk":20,"relays":[10]}])
        self.assertEqual(h.routes["s"][0]["chain"][1],"r")
        self.assertEqual(h.routes["s"][1]["chain"][1],"r2")

    def test_duplicate_ingestion_is_idempotent(self):
        e={"t":self.t,"k":"route","id":"s","nwk":20,"relays":[10]}
        self.add([e]);self.add([e]);self.assertEqual(self.db.execute("SELECT count(*) FROM events").fetchone()[0],1)

    def test_positions_survive_rename_and_new_device(self):
        h=self.add([])
        initial={"version":1,"nodes":{"c":{"x":0,"y":0,"w":100,"h":100},"r":{"x":300,"y":0,"w":150,"h":42},"s":{"x":300,"y":200,"w":150,"h":42}}}
        (self.root/"layout.json").write_text(json.dumps(initial))
        self.devices[2]["name"]="renamed-device"
        self.devices.append({"id":"new","name":"new","nwk":30,"role":"Router"})
        ingest(self.db,{"devices":self.devices})
        result=initial_layout(History(self.db),self.root)
        for key in initial["nodes"]:self.assertEqual(result["nodes"][key],initial["nodes"][key])

    def test_router_avoids_intervening_box(self):
        layout={"nodes":{"a":{"x":0,"y":0,"w":120,"h":42},"obstacle":{"x":250,"y":0,"w":120,"h":140},"b":{"x":500,"y":0,"w":120,"h":42}}}
        points=Router(layout).path("a","b")
        self.assertGreater(len(points),2)
        self.assertFalse(any(intersects(a,b,layout["nodes"]["obstacle"],5) for a,b in zip(points,points[1:])))

    def test_seen_first_and_last_preserve_past_frame(self):
        h=self.add([{"t":self.t,"k":"seen","name":"sensor","point":"first"},
                    {"t":self.t+30,"k":"seen","name":"sensor","point":"last"}])
        self.assertEqual(h.frame(self.t)["nodes"]["s"]["status"],"observed")

    def test_duplicate_does_not_invalidate_cached_history(self):
        e={"t":self.t,"k":"route","id":"s","nwk":20,"relays":[10]}
        self.add([e]);self.db.execute("DELETE FROM settings WHERE key='dirty_since'");self.db.commit()
        self.add([e]);self.assertIsNone(self.db.execute("SELECT value FROM settings WHERE key='dirty_since'").fetchone())

    def test_bounded_reader_keeps_prior_state(self):
        self.add([{"t":self.t,"k":"route","id":"s","nwk":20,"relays":[10]},
                  {"t":self.t+100,"k":"seen","name":"sensor"},
                  {"t":self.t+10000,"k":"seen","name":"sensor"}],
                 [{"start":self.t,"end":self.t+11000,"scope":"all"}])
        full=History(self.db).frame(self.t+10001)
        partial=History(self.db,self.t+9000).frame(self.t+10001)
        self.assertEqual(full["nodes"]["s"],partial["nodes"]["s"])
        self.assertEqual(full["paths"]["s"],partial["paths"]["s"])

    def test_retention_preserves_baseline_and_archived_frame(self):
        self.add([{"t":1,"k":"seen","name":"sensor"},{"t":122,"k":"seen","name":"sensor"},
                  {"t":303,"k":"route","id":"s","nwk":20,"relays":[10]}])
        archived=self.root/"site/frames/old.js";archived.parent.mkdir(parents=True);archived.write_text("archived")
        maintain(self.db,30)
        times=[r[0] for r in self.db.execute("SELECT t FROM events ORDER BY t")]
        self.assertEqual(times,[122,303]);self.assertEqual(archived.read_text(),"archived")

    def test_bounded_history_retains_old_address_identity(self):
        self.add([{"t":self.t,"k":"join","id":"s","nwk":15,"parent_nwk":10,"status":"JOIN"},
                  {"t":self.t+1,"k":"seen","nwk":15},
                  {"t":self.t+100,"k":"join","id":"s","nwk":20,"parent_nwk":10,"status":"JOIN"}])
        h=History(self.db,self.t+200)
        self.assertNotIn("nwk:15",h.devices)
        self.assertEqual(h.resolve(15,self.t+1),"s")


class ReaderTests(unittest.TestCase):
    def test_fall_back_hour_is_monotonic(self):
        previous,_=parse_line("[2026-10-25 02:59:59] debug: ignored")
        ts,_=parse_line("[2026-10-25 02:00:00] debug: ignored",previous)
        self.assertEqual(ts-previous,1)

    def test_new_rotation_in_repeated_hour_uses_file_clock(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/"log.log";p.write_text("[2026-10-25 02:10:00] debug: Received Zigbee message from 'sensor'\n")
            actual=int(dt.datetime(2026,10,25,2,10,tzinfo=ZoneInfo("Europe/Prague"),fold=1).timestamp())
            os.utime(p,(actual,actual));self.assertEqual(read_paths([p])["events"][0]["t"],actual)

    def test_info_only_gap_is_not_radio_coverage(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/"log.log"
            p.write_text("[2026-10-23 12:00:00] debug: Received Zigbee message from 'sensor'\n[2026-10-23 13:00:00] info: MQTT publish cached\n[2026-10-23 14:00:00] debug: Received Zigbee message from 'sensor'\n")
            spans=read_paths([p])["coverage"]
            self.assertEqual(len(spans),2)

    def test_leave_is_not_join(self):
        _,e=parse_line("[2026-10-23 12:00:00] debug: ezspTrustCenterJoinHandler: newNodeId=1 newNodeEui64=0x123 status=DEVICE_LEFT policyDecision=NO_ACTION parentOfNewNodeId=2")
        self.assertEqual(e["k"],"leave")

    def test_cached_mqtt_payload_is_not_radio_receive(self):
        _,e=parse_line("[2026-10-23 12:00:00] info: z2m:mqtt: MQTT publish: topic 'zigbee2mqtt/sensor', payload '{\"last_seen\":\"2026-10-20\"}'")
        self.assertIsNone(e)

    def test_checkpoint_handles_rename_and_partial_line(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/"log.log"
            p.write_text('[2026-10-23 12:00:00] debug: Received Zigbee message from \'sensor\'\npartial')
            first=read_paths([p]);self.assertEqual(len(first["events"]),1)
            q=Path(d)/"log1.log";p.rename(q)
            with q.open("a") as f:f.write(" complete\n[2026-10-23 12:01:00] debug: Received Zigbee message from 'sensor'\n")
            second=read_paths([q],first["checkpoint"])
            self.assertEqual(len(second["events"]),1)
            self.assertGreater(second["events"][0]["t"],first["events"][0]["t"])

    def test_group_name_does_not_replace_last_device(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)
            (p/"configuration.yaml").write_text("devices:\n  '0x123':\n    friendly_name: sensor\ngroups:\n  '2':\n    friendly_name: group\n")
            (p/"database.db").write_text(json.dumps({"ieeeAddr":"0x123","nwkAddr":1,"type":"Router"}))
            self.assertEqual(metadata(p)[0]["name"],"sensor")


if __name__ == "__main__":unittest.main()
