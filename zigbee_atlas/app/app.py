#!/usr/bin/env python3
"""Small standalone HA app: read mounted logs, render and serve via ingress."""
from functools import partial
from http.server import SimpleHTTPRequestHandler,ThreadingHTTPServer
import fcntl
import hashlib
import html
import json
import logging
import os
from pathlib import Path
import shutil
import signal
import threading
import time
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import core
import log_reader

LOG=logging.getLogger("zigbee-atlas")
DEFAULTS={"zigbee2mqtt_directory":"auto","log_directory":"auto","timezone":"Europe/Prague",
          "mqtt_base_topic":"zigbee2mqtt","poll_seconds":60,"render_minutes":60,"detail_days":30}


class SetupError(Exception):
    pass


def options(path):
    values={**DEFAULTS,**(json.loads(path.read_text()) if path.exists() else {})}
    ZoneInfo(values["timezone"])
    for name,low,high in (("poll_seconds",10,300),("render_minutes",1,1440),("detail_days",1,90)):
        if not isinstance(values[name],int) or not low<=values[name]<=high:
            raise SetupError("Neplatná hodnota volby "+name)
    return values


def find_source(configured,roots=None):
    if configured!="auto":
        candidate=Path(configured)
        if not (candidate/"database.db").is_file() or not (candidate/"configuration.yaml").is_file():
            raise SetupError("V nastavené složce chybí database.db nebo configuration.yaml Zigbee2MQTT.")
        return candidate
    candidates=[]
    roots=roots or [Path("/homeassistant"),Path("/addon_configs"),Path("/app_configs"),Path("/share")]
    identities=set()
    for root in roots:
        if not root.is_dir():continue
        for path in [root,*[p for p in root.iterdir() if p.is_dir()]]:
            database=path/"database.db"
            if not database.is_file() or not (path/"configuration.yaml").is_file():continue
            stat=database.stat();identity=(stat.st_dev,stat.st_ino)
            if identity in identities:continue
            try:
                with database.open("rb") as stream:sample=stream.read(4096)
                if b'"ieeeAddr"' not in sample:continue
            except OSError:continue
            identities.add(identity);candidates.append(path)
    if not candidates:raise SetupError("Zigbee2MQTT nenalezen. Nastav zigbee2mqtt_directory na jeho datovou složku.")
    if len(candidates)>1:raise SetupError("Nalezeno více sítí. Vyber zigbee2mqtt_directory: "+", ".join(map(str,candidates)))
    return candidates[0]


def source_metadata(source):
    devices=log_reader.metadata(source)
    try:
        import yaml
        config=yaml.safe_load((source/"configuration.yaml").read_text()) or {}
        definitions=config.get("devices",{})
        if isinstance(definitions,str):
            external=(source/definitions).resolve()
            if not external.is_relative_to(source.resolve()):raise SetupError("Soubor devices musí být uvnitř datové složky Zigbee2MQTT.")
            definitions=yaml.safe_load(external.read_text()) or {}
        for device in devices:
            definition=definitions.get(device["id"],{})
            if isinstance(definition,dict) and definition.get("friendly_name"):
                device["name"]=str(definition["friendly_name"])
        adapter=str(config.get("serial",{}).get("adapter","auto"))
        return devices,adapter
    except SetupError:raise
    except Exception:
        # Never log a YAML parser exception: it can contain a password line.
        return devices,"auto"


def log_files(source,configured):
    roots=[Path(configured)] if configured!="auto" else [source/"log",source/"logs",source]
    for root in roots:
        if not root.is_dir():continue
        direct=list(root.glob("log*.log"))
        folders=sorted((p for p in root.iterdir() if p.is_dir() and list(p.glob("log*.log"))),key=lambda p:p.stat().st_mtime)
        paths=direct+[p for folder in folders[-2:] for p in folder.glob("log*.log")]
        if paths:return paths
    raise SetupError("Logy nebyly nalezeny. V Zigbee2MQTT nastav advanced.log_level: debug a log_output: [console, file].")


def atomic_publish(build,site):
    site.mkdir(parents=True,exist_ok=True)
    count=0
    priority={"manifest.js":1,"status.json":2,"index.html":3}
    for source in sorted((p for p in build.rglob("*") if p.is_file()),key=lambda p:(priority.get(p.name,0),str(p))):
        target=site/source.relative_to(build)
        if target.exists() and target.read_bytes()==source.read_bytes():continue
        target.parent.mkdir(parents=True,exist_ok=True)
        temporary=target.with_name(target.name+".pending")
        shutil.copyfile(source,temporary);os.replace(temporary,target);count+=1
    return count


class Worker:
    def __init__(self,config,data):
        self.config=config;self.data=data;self.site=data/"site";self.build=data/"build"
        self.stop=threading.Event();self.status={"state":"starting","message":"Načítám Zigbee historii…"}
        self.thread=threading.Thread(target=self.run,name="atlas-worker",daemon=True)

    def cycle(self):
        if shutil.disk_usage(self.data).free<500*1024*1024:raise SetupError("Dochází místo: zbývá méně než 500 MiB. Poslední mapa zůstává dostupná.")
        source=find_source(self.config["zigbee2mqtt_directory"])
        paths=log_files(source,self.config["log_directory"])
        checkpoint_path=self.data/"reader-checkpoint.json"
        checkpoint=json.loads(checkpoint_path.read_text()) if checkpoint_path.exists() else {}
        batch=log_reader.read_paths(paths,checkpoint)
        batch["devices"],adapter=source_metadata(source)
        with (self.data/"run.lock").open("w") as lock:
            fcntl.flock(lock,fcntl.LOCK_EX)
            db=core.connect(self.data)
            try:
                core.ingest(db,batch)
                core.dump(checkpoint_path,batch["checkpoint"])
                with db:db.execute("INSERT OR REPLACE INTO settings VALUES('last_poll',?)",(str(batch["read_at"]),))
                has_events=db.execute("SELECT 1 FROM events LIMIT 1").fetchone()
                previous=db.execute("SELECT value FROM settings WHERE key='last_render'").fetchone()
                interval=self.config["render_minutes"]*60
                due=not previous or int(previous[0])//interval!=int(time.time())//interval or not (self.site/"manifest.js").exists()
                if has_events and due:
                    core.render(db,self.data,self.build)
                    path=self.build/"manifest.js"
                    manifest=json.loads(path.read_text().removeprefix("window.ZigbeeManifest=").strip().removesuffix(";"))
                    manifest["timezone"]=self.config["timezone"]
                    manifest["adapter"]=adapter
                    manifest["route_support"]="ember" if any(e[0]=="route" for e in db.execute("SELECT DISTINCT k FROM events")) else "not-observed"
                    path.write_text("window.ZigbeeManifest="+json.dumps(manifest,ensure_ascii=False,separators=(",",":"))+";\n")
                    LOG.info("Mapa aktualizována (%s souborů).",atomic_publish(self.build,self.site))
                    core.maintain(db,self.config["detail_days"])
                routes=db.execute("SELECT count(*) FROM events WHERE k='route'").fetchone()[0]
                message=("Zatím nebyly přečteny Zigbee zprávy. Zkontroluj v Z2M log_level: debug a log_output: [console, file]." if not has_events else
                         "Sběr běží." if routes else "Zprávy se sbírají, celé trasy zatím nebyly pozorovány. Pasivní trasy jsou v této verzi podporované pro Ember/EZSP; pro jiný adaptér může být mapa bez spojů.")
                self.status={"state":"ready" if (self.site/"index.html").exists() else "waiting","message":message,
                             "last_poll":batch["read_at"],"adapter":adapter,"routes":routes,"devices":len(batch["devices"])}
            finally:db.close()

    def run(self):
        previous_error=None
        while not self.stop.is_set():
            try:self.cycle();previous_error=None
            except SetupError as exc:
                self.status={"state":"waiting","message":str(exc)}
                if str(exc)!=previous_error:LOG.warning("%s",exc);previous_error=str(exc)
            except Exception as exc:
                message="Čtení nebo generování se nezdařilo ("+type(exc).__name__+"). Další běh to zkusí znovu."
                self.status={"state":"error","message":message}
                if message!=previous_error:LOG.error("%s",message);previous_error=message
            self.stop.wait(self.config["poll_seconds"])


class Handler(SimpleHTTPRequestHandler):
    def __init__(self,*args,worker,ingress,**kwargs):
        self.worker=worker;self.ingress=ingress
        super().__init__(*args,directory=str(worker.site),**kwargs)

    def log_message(self,format,*args):
        pass

    def end_headers(self):
        self.send_header("Cache-Control","no-store")
        self.send_header("X-Content-Type-Options","nosniff")
        self.send_header("Referrer-Policy","no-referrer")
        super().end_headers()

    def list_directory(self,path):
        self.send_error(403);return None

    def do_HEAD(self):
        if self.ingress and self.client_address[0]!="172.30.32.2":self.send_error(403);return
        super().do_HEAD()

    def do_GET(self):
        if self.ingress and self.client_address[0]!="172.30.32.2":self.send_error(403);return
        path=urlsplit(self.path).path
        if path=="/health":
            payload=json.dumps(self.worker.status,ensure_ascii=False).encode()
            self.send_response(200 if self.worker.thread.is_alive() else 503)
            self.send_header("Content-Type","application/json; charset=utf-8");self.send_header("Content-Length",str(len(payload)))
            self.end_headers();self.wfile.write(payload);return
        if path in ("/","/index.html") and not (self.worker.site/"index.html").exists():
            message=html.escape(self.worker.status["message"])
            payload=("<!doctype html><html lang='cs'><meta charset='utf-8'><meta name='viewport' content='width=device-width'>"
                     "<meta http-equiv='refresh' content='20'><title>Zigbee Atlas</title><body style='font:16px system-ui;max-width:650px;margin:8vh auto;padding:24px;background:#f5f8fc;color:#20394f'>"
                     "<h1>Zigbee Atlas</h1><p>"+message+"</p><p>Stránka se sama obnoví po načtení prvních dat. Atlas čte logy; do Zigbee žádné příkazy neposílá.</p></body></html>").encode()
            self.send_response(200);self.send_header("Content-Type","text/html; charset=utf-8")
            self.send_header("Content-Length",str(len(payload)));self.end_headers();self.wfile.write(payload);return
        if path.endswith(".pending"):self.send_error(404);return
        super().do_GET()


def main():
    logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s %(message)s")
    config=options(Path(os.environ.get("ATLAS_OPTIONS","/data/options.json")))
    core.ZONE=ZoneInfo(config["timezone"])
    os.environ["ATLAS_TIMEZONE"]=config["timezone"]
    os.environ["ATLAS_MQTT_BASE_TOPIC"]=config["mqtt_base_topic"]
    data=Path(os.environ.get("ATLAS_DATA","/data/atlas"));data.mkdir(parents=True,exist_ok=True)
    worker=Worker(config,data);worker.thread.start()
    ingress=os.environ.get("ATLAS_INGRESS","true").lower()!="false"
    server=ThreadingHTTPServer(("0.0.0.0",int(os.environ.get("ATLAS_PORT","8099"))),partial(Handler,worker=worker,ingress=ingress))
    def stop(*_):
        worker.stop.set();threading.Thread(target=server.shutdown,daemon=True).start()
    signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
    LOG.info("Zigbee Atlas naslouchá na portu %s.",server.server_address[1])
    try:server.serve_forever()
    finally:worker.stop.set();server.server_close();worker.thread.join(timeout=30)


if __name__=="__main__":main()
