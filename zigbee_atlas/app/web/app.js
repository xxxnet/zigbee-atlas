"use strict";
(() => {
  const $ = id => document.getElementById(id);
  const NS = "http://www.w3.org/2000/svg";
  const manifest = window.ZigbeeManifest, world = window.ZigbeeWorld;
  if (!manifest?.frames?.length || !world?.nodes) { $("error").hidden=false; $("error").textContent="Chybí data mapy. Spusť generování snímků."; return; }
  if(manifest.route_support==="not-observed"){$("capability").hidden=false;$("capability").textContent="Celé cesty zatím nebyly pozorovány. První verze je čte z debug logů Ember/EZSP; pro jiný adaptér mohou být dostupné jen stavy zařízení.";}
  const svg = $("map"), edgesLayer = make("g"), labelsLayer = make("g"), nodesLayer = make("g");
  const stateNames = {observed:"Nedávno přijato",silent:"Delší ticho",offline:"Z2M: offline",online:"Z2M: online",unknown:"Bez pozorování",unobserved:"Starší pozorování",coordinator:"Koordinátor"};
  let frames=[], position=0, frame=null, selected=null, timer=null, token=0, playSession=0, camera=[...world.bounds], drag=null;
  const nodeElements=new Map(), nodeShapes=new Map();
  const fmt = t => t == null ? "—" : new Date(t*1000).toLocaleString("cs-CZ",{timeZone:manifest.timezone||"Europe/Prague",day:"2-digit",month:"2-digit",hour:"2-digit",minute:"2-digit"});
  function make(tag,attrs={}){ const e=document.createElementNS(NS,tag); for(const [k,v] of Object.entries(attrs)) e.setAttribute(k,String(v)); return e; }
  function label(tag,text,attrs={}){const e=make(tag,attrs);e.textContent=text;return e;}
  function error(message){$("error").textContent=message;$("error").hidden=false;stop();}
  function view(){svg.setAttribute("viewBox",camera.join(" "));}
  function fit(){camera=[...world.bounds];view();}
  function zoom(factor,x=.5,y=.5){const [a,b,w,h]=camera;camera=[a+w*x*(1-factor),b+h*y*(1-factor),w*factor,h*factor];view();}
  function load(meta){
    if(window.ZigbeeFrames[meta.id])return Promise.resolve(window.ZigbeeFrames[meta.id]);
    return new Promise((resolve,reject)=>{const s=document.createElement("script");s.src=meta.file+"?v="+manifest.generated;s.onload=()=>{s.remove();const f=window.ZigbeeFrames[meta.id];f?resolve(f):reject(Error("Prázdný snímek"));};s.onerror=()=>{s.remove();reject(Error("Nelze načíst "+meta.file));};document.head.append(s);});
  }
  const defs=make("defs");
  for(const [name,color] of Object.entries({primary:"#087d9c",historical:"#bbc6d4",parent:"#8055c9",focus:"#087d9c"})){
    const marker=make("marker",{id:"arrow-"+name,viewBox:"0 0 10 10",refX:9,refY:5,markerWidth:5,markerHeight:5,orient:"auto-start-reverse",markerUnits:"strokeWidth"});
    marker.append(make("path",{d:"M0 0L10 5L0 10Z",fill:color}));defs.append(marker);
  }
  svg.append(defs,edgesLayer,labelsLayer,nodesLayer);view();
  for(const [key,node] of Object.entries(world.nodes)){
    const g=make("g",{class:"node",tabindex:0,role:"button","aria-label":node.name,"data-id":key});
    let shape;
    if(["EndDevice","Coordinator"].includes(node.role))shape=make("ellipse",{cx:node.x,cy:node.y,rx:node.w/2,ry:node.h/2});
    else shape=make("rect",{x:node.x-node.w/2,y:node.y-node.h/2,width:node.w,height:node.h,rx:7});
    shape.setAttribute("class","node-body");shape.setAttribute("stroke-width",1.5);
    const name=label("text",node.name,{x:node.x,y:node.y+4,"text-anchor":"middle","font-size":11,"font-family":"system-ui,sans-serif",fill:"#263b51"});
    if(node.name.length*6.7>node.w-15){name.setAttribute("textLength",node.w-16);name.setAttribute("lengthAdjust","spacingAndGlyphs");}
    const title=label("title",node.name+" · "+node.id);
    g.append(title,shape,name);g.addEventListener("click",e=>{e.stopPropagation();choose(key);});g.addEventListener("keydown",e=>{if(e.key==="Enter")choose(key);});
    nodesLayer.append(g);nodeElements.set(key,g);nodeShapes.set(key,shape);
    const option=document.createElement("option");option.value=node.name;$("devices").append(option);
  }
  function related(){
    const result=new Set(selected?[selected]:[]);
    if(selected&&frame){for(const e of frame.edges)if(e.a===selected||e.b===selected||e.sources.includes(selected)){result.add(e.a);result.add(e.b);}}
    return result;
  }
  function render(){
    if(!frame)return;
    const linked=related();edgesLayer.replaceChildren();labelsLayer.replaceChildren();
    const ordered=[...frame.edges].sort((a,b)=>(a.kind!=="historical")-(b.kind!=="historical"));
    for(const edge of ordered){
      if(edge.kind==="historical"&&!$("historical").checked)continue;
      const points=world.edges[edge.id];if(!points)continue;
      const active=!selected||edge.sources.includes(selected)||edge.a===selected||edge.b===selected;
      const kind=edge.kind,colors={primary:"#087d9c",historical:"#bbc6d4",parent:"#8055c9"};
      const width=kind==="historical"?1.25:2+Math.log2(edge.sources.length+1)*.55;
      const path=make("path",{class:"edge",d:"M "+points.map(p=>p.join(",")).join(" L "),fill:"none",stroke:colors[kind],"stroke-width":width,"stroke-linecap":"round","stroke-linejoin":"round",opacity:active?(kind==="historical"?.55:1):.055,"marker-end":"url(#arrow-"+kind+")"});
      if(kind==="parent")path.setAttribute("stroke-dasharray","7 5");
      path.append(label("title",world.nodes[edge.a].name+" → "+world.nodes[edge.b].name+" · "+edge.sources.length+" zdrojů · "+fmt(edge.last)));
      edgesLayer.append(path);
      if($("counts").checked&&active&&kind==="primary"&&edge.sources.length>1){
        let best=0,mid=points[0];for(let i=1;i<points.length;i++){const len=Math.hypot(points[i][0]-points[i-1][0],points[i][1]-points[i-1][1]);if(len>best){best=len;mid=[(points[i][0]+points[i-1][0])/2,(points[i][1]+points[i-1][1])/2];}}
        const g=make("g");g.append(make("rect",{x:mid[0]-9,y:mid[1]-8,width:18,height:16,rx:5,fill:"#f7f9fc"}),label("text",edge.sources.length,{x:mid[0],y:mid[1]+3.5,"text-anchor":"middle","font-size":10,"font-family":"system-ui",fill:"#47728a"}));labelsLayer.append(g);
      }
    }
    for(const [key,node] of Object.entries(world.nodes)){
      const data=frame.nodes[key]||{status:"unknown"};const shape=nodeShapes.get(key),g=nodeElements.get(key);
      let fill=node.role==="Coordinator"?"#ccebdc":node.role==="Router"?"#ffedb5":"#d9edff";
      let stroke="#7f92a9";
      if(["unknown","unobserved"].includes(data.status)){fill="#edf1f6";stroke="#b0bdcc";}
      if(data.status==="silent"){fill="#fff0d2";stroke="#cc8c2d";}
      if(data.status==="offline"){fill="#fde4df";stroke="#cf6658";}
      if(data.events?.some(v=>["parent","rejoin","route"].includes(v)))stroke="#9a6ac9";
      if(data.events?.some(v=>["recovery","online"].includes(v)))stroke="#249b80";
      if(data.status==="silent")stroke="#cc8c2d";
      if(data.status==="offline")stroke="#cf6658";
      if(key===selected)stroke="#087d9c";
      shape.setAttribute("fill",fill);shape.setAttribute("stroke",stroke);shape.setAttribute("stroke-width",key===selected?3.5:data.events?.length?2.7:1.5);
      shape.setAttribute("stroke-dasharray",["silent","unknown","unobserved"].includes(data.status)?"5 4":"none");g.setAttribute("opacity",!selected||linked.has(key)?1:.22);
      g.querySelector("title").textContent=node.name+" · "+(stateNames[data.status]||data.status)+" · poslední zpráva "+fmt(data.last_seen);
    }
    $("count-observed").textContent=frame.counts.observed||0;$("count-silent").textContent=frame.counts.silent||0;$("count-offline").textContent=frame.counts.offline||0;
    $("coverage").textContent="Celosíťový záznam za 24 h: "+Math.round(frame.coverage*100)+" %";
    $("coverage-note").textContent=frame.coverage<.95?"Záznam je částečný. Šedé uzly nemají dost čerstvých dat; nejde o potvrzení výpadku.":"Při vyhodnocení ticha se kontroluje souvislé pokrytí sběrem.";
    $("empty").hidden=Object.values(frame.nodes).some(n=>n.last_seen&&frame.t-n.last_seen<86400);
    inspector();events();
  }
  function inspector(){
    const box=$("inspector");box.replaceChildren();const eyebrow=document.createElement("span");eyebrow.className="eyebrow";eyebrow.textContent="DETAIL ZAŘÍZENÍ";box.append(eyebrow);
    const h=document.createElement("h2");h.textContent=selected?world.nodes[selected].name:"Klikni na uzel";box.append(h);
    if(!selected){const p=document.createElement("p");p.textContent="Vyber zařízení pro jeho cesty, rodiče a poslední komunikaci.";box.append(p);return;}
    const node=world.nodes[selected],data=frame.nodes[selected]||{};
    const status=document.createElement("span");status.className="status";status.textContent=stateNames[data.status]||"Bez dat";box.append(status);
    const dl=document.createElement("dl");
    for(const [key,value] of [["Typ",node.role],["Poslední zpráva",fmt(data.last_seen)],["Poslední trasa",fmt(data.last_route)],[data.parent_confirmed?"Rodič při joinu":"První relay",world.nodes[data.parent]?.name||"—"],["Model",node.model||"—"]]){
      const row=document.createElement("div"),dt=document.createElement("dt"),dd=document.createElement("dd");dt.textContent=key;dd.textContent=value;row.append(dt,dd);dl.append(row);
    }box.append(dl);
    const path=document.createElement("p");path.className="path";path.textContent=(frame.paths[selected]||[]).map(k=>world.nodes[k]?.name||k).join(" → ");box.append(path);
    const ieee=document.createElement("p");ieee.className="ieee";ieee.textContent=node.id;box.append(ieee);
  }
  function events(){
    const list=$("events"),filter=$("event-filter").value;list.replaceChildren();
    const matching=frame.events.filter(e=>(!selected||e.id===selected)&&(filter==="all"||filter==="important"&&(e.type!=="route"||e.parent_changed)||filter==="parent"&&["parent","rejoin"].includes(e.type)||filter==="health"&&["silent","recovery","offline","online","leave"].includes(e.type)||filter===e.type));
    $("event-count").textContent=matching.reduce((a,e)=>a+(e.count||1),0);
    for(const event of matching){
      const button=document.createElement("button");button.className="event";const dot=document.createElement("span");dot.className="event-dot "+event.type;
      const text=document.createElement("span");text.className="event-text";const name=document.createElement("strong");name.textContent=world.nodes[event.id]?.name||"Síť";const detail=document.createElement("span");detail.textContent=event.text+(event.parent?" · "+(world.nodes[event.parent]?.name||event.parent):"")+(event.count>1?" · "+event.count+"×":"");text.append(name,detail);
      const when=document.createElement("span");when.className="event-time";when.textContent=fmt(event.t);button.append(dot,text,when);button.addEventListener("click",()=>{if(event.id)choose(event.id,true);});list.append(button);
    }
    if(!matching.length){const p=document.createElement("p");p.className="none";p.textContent="V tomto výběru nejsou zaznamenané události.";list.append(p);}
  }
  function choose(key,focus=false){selected=key;$("clear-selection").hidden=!key;$("search").value=key?world.nodes[key].name:"";render();if(key&&focus){const route=(frame?.paths[key]||[key]).map(k=>world.nodes[k]).filter(Boolean);const left=Math.min(...route.map(n=>n.x-n.w/2))-100,right=Math.max(...route.map(n=>n.x+n.w/2))+100,top=Math.min(...route.map(n=>n.y-n.h/2))-100,bottom=Math.max(...route.map(n=>n.y+n.h/2))+100;const ratio=svg.clientWidth/Math.max(svg.clientHeight,1);const w=Math.max(650,right-left,(bottom-top)*ratio),h=w/ratio;camera=[(left+right-w)/2,(top+bottom-h)/2,w,h];view();}}
  async function select(index){
    position=Math.max(0,Math.min(index,frames.length-1));const meta=frames[position],version=++token;
    $("timeline").value=position;$("position").textContent=(position+1)+" / "+frames.length;$("frame-title").textContent=meta.label;$("loading").hidden=false;
    $("previous").disabled=position===0;$("next").disabled=position===frames.length-1;
    try{const loaded=await load(meta);if(version!==token)return;frame=loaded;render();const next=frames[position+1];if(next)load(next).catch(()=>{});}catch(e){if(version===token)error(e.message);}finally{if(version===token)$("loading").hidden=true;}
  }
  function resolution(){const previous=frame?.t||Infinity;frames=manifest.frames.filter(f=>f.tags.includes($("resolution").value)||f.tags.includes("latest"));$("timeline").max=frames.length-1;$("range-start").textContent=frames[0].label;$("range-end").textContent=frames.at(-1).label;let i=frames.findIndex(f=>f.t>=previous);select(i<0?frames.length-1:i);}
  function stop(){playSession++;if(timer)clearTimeout(timer);timer=null;$("play").textContent="▶";$("play").setAttribute("aria-label","Přehrát");}
  function play(){stop();const session=playSession;$("play").textContent="Ⅱ";$("play").setAttribute("aria-label","Pozastavit");const tick=async()=>{if(session!==playSession)return;if(position>=frames.length-1){if(!$("loop").checked){stop();return;}await select(0);}else await select(position+1);if(session===playSession)timer=setTimeout(tick,Number($("speed").value));};timer=setTimeout(tick,Number($("speed").value));}
  $("play").onclick=()=>timer?stop():play();$("speed").onchange=()=>{if(timer)play();};$("previous").onclick=()=>{stop();select(position-1);};$("next").onclick=()=>{stop();select(position+1);};$("timeline").oninput=()=>{stop();select(Number($("timeline").value));};$("resolution").onchange=()=>{stop();resolution();};
  for(const id of ["historical","counts","event-filter"])$(id).onchange=render;
  $("clear-selection").onclick=()=>choose(null);$("search").onchange=()=>{const q=$("search").value.toLowerCase();const key=Object.keys(world.nodes).find(k=>world.nodes[k].name.toLowerCase()===q)||Object.keys(world.nodes).find(k=>world.nodes[k].name.toLowerCase().includes(q));if(key&&q)choose(key,true);if(!q)choose(null);};
  $("zoom-in").onclick=()=>zoom(.8);$("zoom-out").onclick=()=>zoom(1.25);$("fit").onclick=fit;$("refresh").onclick=()=>{const u=new URL(location.href);u.searchParams.set("refresh",Date.now());location.href=u.href;};
  $("freshness").textContent="Data do "+fmt(manifest.data_until);$("freshness").title="Vygenerováno "+fmt(manifest.generated);
  $("download").onclick=()=>{const clone=svg.cloneNode(true);clone.setAttribute("viewBox",world.bounds.join(" "));clone.setAttribute("xmlns",NS);clone.setAttribute("width",world.bounds[2]);clone.setAttribute("height",world.bounds[3]);const link=document.createElement("a"),url=URL.createObjectURL(new Blob([new XMLSerializer().serializeToString(clone)],{type:"image/svg+xml;charset=utf-8"}));link.href=url;link.download="zigbee-"+new Date(frame.t*1000).toISOString().replaceAll(":","-")+".svg";link.click();setTimeout(()=>URL.revokeObjectURL(url),1000);};
  svg.addEventListener("wheel",e=>{e.preventDefault();const box=svg.getBoundingClientRect();zoom(e.deltaY>0?1.12:.89,(e.clientX-box.left)/box.width,(e.clientY-box.top)/box.height);},{passive:false});
  svg.addEventListener("pointerdown",e=>{if(e.target.closest(".node"))return;drag={x:e.clientX,y:e.clientY,camera:[...camera]};svg.setPointerCapture(e.pointerId);svg.classList.add("dragging");});
  svg.addEventListener("pointermove",e=>{if(!drag)return;camera=[drag.camera[0]-(e.clientX-drag.x)*camera[2]/svg.clientWidth,drag.camera[1]-(e.clientY-drag.y)*camera[3]/svg.clientHeight,camera[2],camera[3]];view();});
  const release=()=>{drag=null;svg.classList.remove("dragging");};svg.addEventListener("pointerup",release);svg.addEventListener("pointercancel",release);
  window.addEventListener("keydown",e=>{if(["INPUT","SELECT","TEXTAREA"].includes(e.target.tagName))return;if(e.key===" "){e.preventDefault();$("play").click();}if(e.key==="ArrowLeft")$("previous").click();if(e.key==="ArrowRight")$("next").click();if(e.key==="Escape")choose(null);});
  window.ZigbeePlayer={getState:()=>({position,frame:frame?.t,selected,playing:!!timer,resolution:$("resolution").value}),select,choose};
  resolution();
})();
