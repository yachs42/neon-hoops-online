'use strict';
// NEON HOOPS: low-latency room server with temporary disconnect recovery.
// The host browser owns game physics. This service relays inputs and small snapshots.
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { URL } = require('node:url');
const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || '0.0.0.0';
const ROOM_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const REJOIN_MS = 45000;
const HEARTBEAT_MS = Number(process.env.HEARTBEAT_MS) || 15000;
const PEER_TIMEOUT_MS = Number(process.env.PEER_TIMEOUT_MS) || 50000;
const MAX_PAYLOAD = 150 * 1024;
const MAX_BUFFER = 350 * 1024;
const MAX_SNAPSHOT_BACKLOG = 48 * 1024;
const rooms = new Map();
const peers = new Set();
const SAFE_INPUTS = new Set(['Space','KeyA','KeyD','ArrowLeft','ArrowRight','ShiftLeft','ShiftRight','KeyQ','KeyW']);
const allowedSfx = new Set(['jump','dash','grab','shot','score','hit','win','dunk','perfect','fail','block','glass','steal','click']);
const gameHtml = fs.readFileSync(path.join(__dirname, 'index.html'));
const timestamp = () => Date.now();
function newCode(){let code;do{const buf=crypto.randomBytes(6);code=Array.from(buf, b=>ROOM_ALPHABET[b%ROOM_ALPHABET.length]).join('')}while(rooms.has(code));return code;}
function respond(res,status,body,contentType){res.writeHead(status,{'Content-Type':contentType,'Cache-Control':'no-store','X-Content-Type-Options':'nosniff'});res.end(body)}
const httpServer=http.createServer((req,res)=>{
 let url;try{url=new URL(req.url,'http://localhost')}catch{return respond(res,400,'Bad request','text/plain')}
 if(req.method==='GET'&&(url.pathname==='/'||url.pathname==='/index.html'))return respond(res,200,gameHtml,'text/html; charset=utf-8');
 if(req.method==='GET'&&url.pathname==='/health')return respond(res,200,JSON.stringify({ok:true,rooms:rooms.size}), 'application/json');
 return respond(res,404,'Not found','text/plain; charset=utf-8');
});
function sendFrame(peer,opcode,data=Buffer.alloc(0),droppable=false){
 if(!peer||peer.closed||peer.socket.destroyed||!peer.socket.writable)return false;
 const socket=peer.socket;
 if(droppable&&socket.writableLength>MAX_SNAPSHOT_BACKLOG)return false;
 // Do not accumulate unlimited stale snapshots if the connection is stuck.
 if(socket.writableLength>2*1024*1024){socket.destroy();return false;}
 const payload=Buffer.isBuffer(data)?data:Buffer.from(String(data));
 const n=payload.length;
 const header=Buffer.alloc(n<126?2:n<=65535?4:10);
 header[0]=0x80|opcode;
 if(n<126)header[1]=n;
 else if(n<=65535){header[1]=126;header.writeUInt16BE(n,2)}
 else{header[1]=127;header.writeBigUInt64BE(BigInt(n),2)}
 try{socket.write(Buffer.concat([header,payload]));return true}catch{socket.destroy();return false}
}
function send(peer,msg,droppable=false){return sendFrame(peer,1,JSON.stringify(msg),droppable)}
function detachPeer(peer,explicit=false){
 if(peer.closed)return;
 peer.closed=true;peers.delete(peer);
 const room=peer.room;peer.room=null;
 if(!room)return;
 const slot=peer.role;
 if(room.slots[slot]!==peer)return;
 room.slots[slot]=null;
 if(explicit){
  if(slot===0){send(room.slots[1],{type:'host_left'});if(room.slots[1])room.slots[1].room=null;rooms.delete(room.code)}
  else{room.tokens[1]=null;room.disconnected[1]=0;room.started=false;room.rematches.clear();send(room.slots[0],{type:'peer_left'})}
 }else{
  room.disconnected[slot]=timestamp();
  const other=room.slots[1-slot];
  if(other)send(other,{type:'peer_disconnected',player:slot,graceMs:REJOIN_MS});
 }
}
function sendStart(room){
 if(!room.slots[0]||!room.slots[1])return;
 room.started=true;room.rematches.clear();
 send(room.slots[0],{type:'start'});send(room.slots[1],{type:'start'});
}
function attach(peer,room,slot){
 room.slots[slot]=peer;room.disconnected[slot]=0;
 peer.room=room;peer.role=slot;
}
function handleMessage(peer,raw){
 let msg;try{msg=JSON.parse(raw.toString('utf8'))}catch{return}
 if(!msg||typeof msg!=='object'||typeof msg.type!=='string')return;
 peer.lastSeen=timestamp();
 if(msg.type==='ping')return send(peer,{type:'pong',t:Number.isFinite(msg.t)?msg.t:0});
 if(msg.type==='create'&&!peer.room){
  const c=newCode(),room={code:c,slots:[null,null],tokens:[crypto.randomBytes(16).toString('hex'),null],disconnected:[0,0],started:false,rematches:new Set()};
  rooms.set(c,room);attach(peer,room,0);
  console.log('room created',c);
  return send(peer,{type:'created',code:c,player:0,token:room.tokens[0]});
 }
 if(msg.type==='join'&&!peer.room){
  const c=String(msg.code||'').trim().toUpperCase(),room=rooms.get(c);
  if(!room)return send(peer,{type:'error',message:'Комната не найдена.'});
  if(room.tokens[1])return send(peer,{type:'error',message:'В комнате уже есть второй игрок.'});
  if(!room.slots[0])return send(peer,{type:'error',message:'Создатель переподключается. Попробуй через несколько секунд.'});
  room.tokens[1]=crypto.randomBytes(16).toString('hex');attach(peer,room,1);
  console.log('room joined',c);
  send(peer,{type:'joined',code:c,player:1,token:room.tokens[1]});sendStart(room);return;
 }
 if(msg.type==='resume'&&!peer.room){
  const c=String(msg.code||'').trim().toUpperCase(),room=rooms.get(c);
  const token=String(msg.token||'');const slot=room?.tokens.indexOf(token)??-1;
  if(!room||token.length!==32||slot<0)return send(peer,{type:'error',fatal:true,message:'Не удалось восстановить комнату.'});
  if(room.disconnected[slot]&&timestamp()-room.disconnected[slot]>REJOIN_MS)return send(peer,{type:'error',fatal:true,message:'Время восстановления комнаты истекло.'});
  // A broken TCP connection can look alive on the server long after the browser detects it.
  // The secret reconnect token lets the new socket safely replace that stale connection.
  const stale=room.slots[slot];
  if(stale){send(stale,{type:'session_taken_over'});stale.room=null;stale.closed=true;peers.delete(stale);stale.socket.destroy()}
  attach(peer,room,slot);console.log('room resumed',c,'player',slot);
  const both=!!(room.slots[0]&&room.slots[1]);
  send(peer,{type:'resumed',code:c,player:slot,token:room.tokens[slot],active:room.started&&both});
  if(both){send(room.slots[1-slot],{type:'peer_reconnected'});send(room.slots[0],{type:'resync_request'});}
  return;
 }
 const room=peer.room;
 if(!room)return;
 if(msg.type==='leave'){detachPeer(peer,true);try{peer.socket.end()}catch{};return}
 if(msg.type==='input'&&peer===room.slots[1]&&SAFE_INPUTS.has(msg.code)&&typeof msg.down==='boolean'){
  // INPUTS: forward immediately, never behind a state queue.
  return send(room.slots[0],{type:'input',code:msg.code,down:msg.down});
 }
 if(msg.type==='snapshot'&&peer===room.slots[0]&&room.slots[1]&&msg.state&&typeof msg.state==='object'){
  // Forward original JSON bytes without serializing them again. Old state frames may be dropped.
  return sendFrame(room.slots[1],1,raw,true);
 }
 if(msg.type==='sfx'&&peer===room.slots[0]&&room.slots[1]&&allowedSfx.has(msg.name))return send(room.slots[1],{type:'sfx',name:msg.name});
 if(msg.type==='rematch'&&room.slots[1]){
  room.rematches.add(peer);
  if(room.rematches.size===2)sendStart(room);
  else send(room.slots[peer.role===0?1:0],{type:'rematch_offer'});
 }
}
function parseData(peer,chunk){
 if(peer.closed)return;
 peer.buffer=Buffer.concat([peer.buffer,chunk]);
 if(peer.buffer.length>MAX_BUFFER){peer.socket.destroy();return}
 while(peer.buffer.length>=2){
  const b=peer.buffer,opcode=b[0]&0x0f,fin=!!(b[0]&0x80),masked=!!(b[1]&0x80);
  let size=b[1]&127,pos=2;
  if(!masked)return peer.socket.destroy();
  if(size===126){if(b.length<4)return;size=b.readUInt16BE(2);pos=4}
  if(size===127){if(b.length<10)return;const n=b.readBigUInt64BE(2);if(n>BigInt(MAX_PAYLOAD))return peer.socket.destroy();size=Number(n);pos=10}
  if(size>MAX_PAYLOAD)return peer.socket.destroy();
  if(b.length<pos+4+size)return;
  const mask=b.subarray(pos,pos+4),payload=Buffer.from(b.subarray(pos+4,pos+4+size));
  for(let i=0;i<size;i++)payload[i]^=mask[i&3];
  peer.buffer=b.subarray(pos+4+size);peer.lastSeen=timestamp();
  if(opcode===8){sendFrame(peer,8);peer.socket.end();return}
  if(opcode===9){sendFrame(peer,10,payload);continue}
  if(opcode===10)continue;
  if(opcode===1&&fin){handleMessage(peer,payload);continue}
  // A browser sends complete text frames. Reject binary/fragmented/invalid data.
  return peer.socket.destroy();
 }
}
httpServer.on('upgrade',(req,socket,head)=>{
 try{
  const url=new URL(req.url,'http://localhost');const key=req.headers['sec-websocket-key'];
  if(url.pathname!=='/ws'||req.headers.upgrade?.toLowerCase()!=='websocket'||typeof key!=='string'||Buffer.from(key,'base64').length!==16){socket.destroy();return}
  const accept=crypto.createHash('sha1').update(key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.setNoDelay(true);socket.setKeepAlive(true,15000);
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+accept+'\r\n\r\n');
  const peer={socket,room:null,role:-1,buffer:Buffer.alloc(0),closed:false,lastSeen:timestamp()};
  peers.add(peer);
  socket.on('data',part=>parseData(peer,part));socket.on('close',()=>detachPeer(peer));socket.on('error',()=>detachPeer(peer));
  if(head.length)parseData(peer,head);
 }catch{socket.destroy()}
});
setInterval(()=>{
 const now=timestamp();
 for(const peer of peers){
  if(now-peer.lastSeen>PEER_TIMEOUT_MS){console.log('peer heartbeat timeout',peer.room?.code||'-');peer.socket.destroy();continue}
  sendFrame(peer,9,Buffer.from('ping'));
 }
 for(const [code,room] of rooms){
  const hostGone=room.disconnected[0]&&now-room.disconnected[0]>=REJOIN_MS;
  const guestGone=room.disconnected[1]&&now-room.disconnected[1]>=REJOIN_MS;
  if(hostGone){send(room.slots[1],{type:'host_left'});if(room.slots[1]){room.slots[1].room=null;room.slots[1].socket.end()}rooms.delete(code);console.log('room expired',code);continue}
  if(guestGone){room.tokens[1]=null;room.disconnected[1]=0;room.started=false;room.rematches.clear();send(room.slots[0],{type:'peer_left'});console.log('guest slot released',code)}
 }
},HEARTBEAT_MS).unref();
httpServer.listen(PORT,HOST,()=>console.log('NEON HOOPS ONLINE: listening on '+PORT));
