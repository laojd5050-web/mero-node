const http = require('http');
const net = require('net');
const { WebSocketServer } = require('ws');

const UUID_HEX = (process.env.UUID || '0aa557b0-dbf6-45af-9f17-47dad89ebf08').replace(/-/g, '');
const PORT = process.env.SERVER_PORT || process.env.PORT || 8080;

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('node ok\n');
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  let first = true;
  let target = null;
  let buffered = [];
  ws.on('message', (data) => {
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    if (!first) {
      if (target && target.writable) target.write(buf);
      else if (buffered) buffered.push(buf);
      return;
    }
    first = false;
    try {
      if (buf.length < 24) { ws.close(); return; }
      if (buf.subarray(1, 17).toString('hex') !== UUID_HEX) { ws.close(); return; }
      let offset = 17;
      const addonLen = buf[offset]; offset += 1 + addonLen;
      const cmd = buf[offset]; offset += 1;
      if (cmd !== 1) { ws.close(); return; }
      const port = buf.readUInt16BE(offset); offset += 2;
      const atyp = buf[offset]; offset += 1;
      let host;
      if (atyp === 1) { host = Array.from(buf.subarray(offset, offset + 4)).join('.'); offset += 4; }
      else if (atyp === 2) { const len = buf[offset]; offset += 1; host = buf.subarray(offset, offset + len).toString(); offset += len; }
      else if (atyp === 3) { const p = []; for (let i = 0; i < 8; i++) p.push(buf.readUInt16BE(offset + i * 2).toString(16)); host = p.join(':'); offset += 16; }
      else { ws.close(); return; }
      target = net.connect(port, host, () => {
        ws.send(Buffer.from([buf[0], 0]));
        const rest = buf.subarray(offset);
        if (rest.length) target.write(rest);
        if (buffered) { for (const b of buffered) target.write(b); buffered = null; }
      });
      target.on('data', (chunk) => { if (ws.readyState === 1) ws.send(chunk); });
      target.on('error', () => { try { ws.close(); } catch (e) {} });
      target.on('close', () => { try { ws.close(); } catch (e) {} });
    } catch (e) { try { ws.close(); } catch (e2) {} }
  });
  ws.on('close', () => { if (target) target.destroy(); });
  ws.on('error', () => { if (target) target.destroy(); });
});

server.listen(PORT, '0.0.0.0', () => console.log('listening on ' + PORT));
