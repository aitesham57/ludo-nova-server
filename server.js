const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const crypto = require('crypto');
const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*', methods: ['GET','POST'] } });
const PORT = process.env.PORT || 3000;
const queue = [];
const rooms = new Map();
const socketRoom = new Map();
const codeFor = () => crypto.randomBytes(3).toString('hex').toUpperCase();
function roomFor(socket){const id=socketRoom.get(socket.id);return id?rooms.get(id):null}
function addPlayer(room,socket,name){room.players.push({id:socket.id,name:(String(name||'Player').slice(0,24)),index:room.players.length});socketRoom.set(socket.id,room.id);socket.join(room.id)}
function freshState(){return {turn:0,dice:0,rolled:false,tokens:{red:[-1,-1,-1,-1],green:[-1,-1,-1,-1],yellow:[-1,-1,-1,-1],blue:[-1,-1,-1,-1]},finished:null,rewarded:false}}
function startRoom(room,eventName='match:found'){if(room.players.length!==2||room.started)return;room.started=true;room.rematchReady=new Set();room.state=freshState();room.players.forEach(p=>io.to(p.id).emit(eventName,{roomId:room.id,playerIndex:p.index,names:[room.players[0].name,room.players[1].name],state:room.state}));}
app.get('/',(req,res)=>res.send('LUDO NOVA realtime server is running. Open the game and connect to this URL.'));
app.get('/health',(req,res)=>res.json({ok:true,service:'ludo-nova-server',queued:queue.length,rooms:rooms.size}));
io.on('connection',socket=>{
 socket.on('queue:join',data=>{if(socketRoom.has(socket.id))return;const existing=queue.findIndex(s=>s.connected&&s.id!==socket.id);if(existing>=0){const other=queue.splice(existing,1)[0];const room={id:'match-'+codeFor(),code:null,players:[],started:false,state:null};rooms.set(room.id,room);addPlayer(room,other,other.data.playerName||'Player');addPlayer(room,socket,data&&data.name||'Player');startRoom(room)}else{socket.data.playerName=String(data&&data.name||'Player').slice(0,24);queue.push(socket);socket.emit('queue:waiting')}});
 socket.on('queue:leave',()=>{const i=queue.findIndex(s=>s.id===socket.id);if(i>=0)queue.splice(i,1);socket.emit('queue:left')});
 socket.on('room:create',data=>{if(socketRoom.has(socket.id))return;let code=codeFor();while([...rooms.values()].some(r=>r.code===code))code=codeFor();const room={id:'private-'+code,code,players:[],started:false,state:null};rooms.set(room.id,room);addPlayer(room,socket,data&&data.name||'Player');socket.emit('room:created',{roomId:room.id,code})});
 socket.on('room:join',data=>{if(socketRoom.has(socket.id))return;const code=String(data&&data.code||'').toUpperCase();const room=[...rooms.values()].find(r=>r.code===code);if(!room){socket.emit('room:error',{message:'Room not found. Check the code.'});return}if(room.started||room.players.length>=2){socket.emit('room:error',{message:'Room is already full.'});return}addPlayer(room,socket,data&&data.name||'Player');startRoom(room)});
 socket.on('match:state',data=>{const room=roomFor(socket);if(!room||!room.started||!data||data.roomId!==room.id)return;const player=room.players.find(p=>p.id===socket.id);if(!player||player.index!==room.state.turn)return;const s=data.state;if(!s||!s.tokens||!['red','green','yellow','blue'].every(c=>Array.isArray(s.tokens[c])&&s.tokens[c].length===4))return;room.state={turn:Number(s.turn)||0,dice:Number(s.dice)||0,rolled:!!s.rolled,tokens:s.tokens,finished:s.finished||null,rewarded:!!s.rewarded};socket.to(room.id).emit('match:state',{state:room.state});if(room.state.finished){io.to(room.id).emit('match:ended',{message:'Match finished. Both players can request a rematch.'});room.started=false;room.rematchReady=new Set()}});
 
 socket.on('match:chat:send',data=>{
  const room=roomFor(socket);
  if(!room||!room.started||!data)return;
  const player=room.players.find(p=>p.id===socket.id);
  if(!player)return;
  const message=String(data.message||'').trim().slice(0,300);
  if(!message)return;
  socket.to(room.id).emit('match:chat:message',{
   name:player.name,
   message
  });
 });
 
 socket.on('match:rematch:ready',data=>{const room=roomFor(socket);if(!room||room.players.length!==2||room.started||!room.state||!room.state.finished)return;room.rematchReady=room.rematchReady||new Set();room.rematchReady.add(socket.id);io.to(room.id).emit('match:rematch:status',{ready:room.rematchReady.size,needed:2,message:room.rematchReady.size===2?'Both players ready. Starting rematch…':'Waiting for opponent to accept rematch…'});if(room.rematchReady.size===2){room.started=false;room.state=freshState();room.rematchReady.clear();startRoom(room,'match:rematch:started')}})
 socket.on('match:rematch:cancel',()=>{const room=roomFor(socket);if(!room||!room.rematchReady)return;room.rematchReady.delete(socket.id);io.to(room.id).emit('match:rematch:status',{ready:room.rematchReady.size,needed:2,message:'A player cancelled the rematch.'})});
 socket.on('disconnect',()=>{const i=queue.findIndex(s=>s.id===socket.id);if(i>=0)queue.splice(i,1);const room=roomFor(socket);socketRoom.delete(socket.id);if(room){room.players=room.players.filter(p=>p.id!==socket.id);if(room.started){io.to(room.id).emit('match:ended',{message:'Opponent disconnected. Online match ended.'});room.started=false}if(!room.players.length)rooms.delete(room.id)}});
});
server.listen(PORT,()=>console.log(`LUDO NOVA server listening on ${PORT}`));
