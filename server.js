const express = require("express");
const http = require("http");
const { WebSocketServer } = require("ws");
const crypto = require("crypto");
const path = require("path");

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });
app.use(express.static(path.join(__dirname, "public")));
app.get("/api/ice", (req,res) => {
  const iceServers = [
    { urls: "stun:stun.l.google.com:19302" },
    { urls: "stun:stun1.l.google.com:19302" }
  ];
  // TURN credentials are supplied by the host environment, never hard-coded in the client.
  if (process.env.TURN_URL && process.env.TURN_USERNAME && process.env.TURN_CREDENTIAL) {
    iceServers.push({
      urls: process.env.TURN_URL.split(",").map(x => x.trim()).filter(Boolean),
      username: process.env.TURN_USERNAME,
      credential: process.env.TURN_CREDENTIAL
    });
  }
  res.setHeader("Cache-Control","no-store");
  res.json({ iceServers, turnConfigured: iceServers.length > 2 });
});
const rooms = new Map(); // code -> { owner, peer, expires }
const clients = new Map();
const PORT = process.env.PORT || 3000;
const TTL = 10 * 60 * 1000;

function code6(){ return String(crypto.randomInt(0,1000000)).padStart(6,"0"); }
function send(ws,obj){ if(ws && ws.readyState===1) ws.send(JSON.stringify(obj)); }
function removeRoom(ws){
  const code=clients.get(ws); if(!code) return;
  const r=rooms.get(code);
  if(r){
    const other=r.owner===ws?r.peer:r.owner;
    send(other,{type:"peer-left"});
    if(r.owner===ws && r.peer) clients.delete(r.peer);
    if(r.peer===ws && r.owner) clients.delete(r.owner);
    rooms.delete(code);
  }
  clients.delete(ws);
}
wss.on("connection",(ws)=>{
  ws.on("message",(raw)=>{
    let m; try{m=JSON.parse(raw.toString())}catch{return}
    if(m.type==="create"){
      let code; do{code=code6()}while(rooms.has(code));
      rooms.set(code,{owner:ws,peer:null,expires:Date.now()+TTL});
      clients.set(ws,code); send(ws,{type:"created",code}); return;
    }
    if(m.type==="join"){
      const code=String(m.code||"");
      const r=rooms.get(code);
      if(!/^\d{6}$/.test(code)||!r||r.expires<Date.now()||r.peer){
        send(ws,{type:"error",message:"ไม่พบรหัส หรือรหัสหมดอายุ/มีคนใช้งานแล้ว"}); return;
      }
      r.peer=ws; clients.set(ws,code);
      send(ws,{type:"joined"}); send(r.owner,{type:"peer-joined"}); return;
    }
    const code=clients.get(ws),r=rooms.get(code); if(!r)return;
    const target=r.owner===ws?r.peer:r.owner;
    if(["offer","answer","ice"].includes(m.type)) send(target,m);
    if(m.type==="leave") removeRoom(ws);
  });
  ws.on("close",()=>removeRoom(ws));
  ws.on("error",()=>removeRoom(ws));
});
setInterval(()=>{for(const [code,r] of rooms){if(r.expires<Date.now()){send(r.owner,{type:"error",message:"รหัสหมดอายุแล้ว"});send(r.peer,{type:"error",message:"รหัสหมดอายุแล้ว"});rooms.delete(code);if(r.owner)clients.delete(r.owner);if(r.peer)clients.delete(r.peer)}}},30000);
server.listen(PORT,"0.0.0.0",()=>console.log(`NEXUS DROP signaling server on port ${PORT}`));
