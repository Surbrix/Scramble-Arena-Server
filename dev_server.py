"""Scramble Arena: local signalling / room / matchmaking server (same JSON protocol as the Cloudflare Worker, see docs/ONLINE-PLAN.md).
Only used for development and testing on this PC. Run:
    tools\\tts-env\\Scripts\\python.exe -I server\\dev_server.py          (listens on ws://127.0.0.1:8790)
It relays WebRTC signalling messages; the game itself runs peer to peer (host-authoritative), nothing about the match goes through here."""
import asyncio, json, os, random, string, sys
import websockets
from http import HTTPStatus

PORT = 8790
MAX_PLAYERS = 4
LETTERS = [c for c in string.ascii_uppercase if c not in 'IO']
clients = {}   # id -> {'ws','name','room'}
rooms = {}     # code -> {'host': id, 'members': [id, ...]}
queue = []     # ids waiting for a quick match
_next = 0


def new_id():
    global _next
    _next += 1
    return 'p%d' % _next


def new_code():
    while True:
        c = ''.join(random.choice(LETTERS) for _ in range(4))
        if c not in rooms:
            return c


async def send(cid, msg):
    c = clients.get(cid)
    if c:
        try:
            await c['ws'].send(json.dumps(msg))
        except Exception:
            pass


def room_info(code):
    r = rooms[code]
    return {'t': 'room', 'code': code, 'host': r['host'], 'players': [{'id': m, 'name': clients[m]['name']} for m in r['members'] if m in clients]}


async def broadcast_room(code):
    for m in list(rooms[code]['members']):
        await send(m, room_info(code))


async def leave(cid):
    c = clients.get(cid)
    if not c:
        return
    if cid in queue:
        queue.remove(cid)
    code = c.get('room')
    c['room'] = None
    if code and code in rooms:
        r = rooms[code]
        if cid in r['members']:
            r['members'].remove(cid)
        if r['host'] == cid:          # host left: the match is over for everybody
            for m in list(r['members']):
                clients[m]['room'] = None
                await send(m, {'t': 'closed', 'code': code, 'why': 'host-left'})
            del rooms[code]
        else:
            for m in r['members']:
                await send(m, {'t': 'peer-left', 'id': cid})
            await broadcast_room(code)


async def try_match(size):
    while len(queue) >= size:
        group = [queue.pop(0) for _ in range(size)]
        code = new_code()
        rooms[code] = {'host': group[0], 'members': list(group)}
        for m in group:
            clients[m]['room'] = code
        for m in group:
            await send(m, {'t': 'matched', 'code': code, 'id': m, 'hostId': group[0], 'role': 'host' if m == group[0] else 'client'})
        await broadcast_room(code)
        for m in group[1:]:   # tell the host to start the WebRTC handshake with every matched player
            await send(group[0], {'t': 'peer-joined', 'id': m, 'name': clients[m]['name']})


async def handler(ws):
    cid = new_id()
    clients[cid] = {'ws': ws, 'name': 'Player', 'room': None}
    await send(cid, {'t': 'hello', 'id': cid})
    try:
        async for raw in ws:
            try:
                m = json.loads(raw)
            except Exception:
                continue
            t = m.get('t')
            c = clients[cid]
            if 'name' in m:
                c['name'] = str(m['name'])[:16] or 'Player'
            if t == 'host':
                await leave(cid)
                code = new_code()
                rooms[code] = {'host': cid, 'members': [cid]}
                c['room'] = code
                await send(cid, {'t': 'hosted', 'code': code, 'id': cid})
                await broadcast_room(code)
            elif t == 'join':
                code = str(m.get('code', '')).upper().strip()
                r = rooms.get(code)
                if not r:
                    await send(cid, {'t': 'error', 'msg': 'No room with that code.'})
                elif len(r['members']) >= MAX_PLAYERS:
                    await send(cid, {'t': 'error', 'msg': 'That room is full.'})
                else:
                    await leave(cid)
                    r['members'].append(cid)
                    c['room'] = code
                    await send(cid, {'t': 'joined', 'code': code, 'id': cid, 'hostId': r['host']})
                    await send(r['host'], {'t': 'peer-joined', 'id': cid, 'name': c['name']})
                    await broadcast_room(code)
            elif t == 'queue':
                await leave(cid)
                size = max(2, min(MAX_PLAYERS, int(m.get('size', 2))))
                queue.append(cid)
                await send(cid, {'t': 'queued', 'waiting': len(queue)})
                await try_match(size)
            elif t == 'unqueue':
                if cid in queue:
                    queue.remove(cid)
                await send(cid, {'t': 'unqueued'})
            elif t == 'signal':
                to = m.get('to')
                tc = clients.get(to)
                # only relay between members of the same room
                if tc and tc.get('room') and tc['room'] == c.get('room'):
                    await send(to, {'t': 'signal', 'from': cid, 'data': m.get('data')})
            elif t == 'leave':
                await leave(cid)
                await send(cid, {'t': 'left'})
            elif t == 'ping':
                await send(cid, {'t': 'pong', 'ts': m.get('ts')})
    finally:
        await leave(cid)
        clients.pop(cid, None)


def health(connection, request):
    # plain HTTP requests (hosting health checks, a browser) get a friendly answer; websocket upgrades continue to the handler
    if request.headers.get('Upgrade', '').lower() != 'websocket':
        return connection.respond(HTTPStatus.OK, 'Scramble Arena game server is running.\n')
    return None


async def main():
    # local: ws://127.0.0.1:8790.  Online hosts set PORT (and we then listen on all addresses).
    port = int(os.environ.get('PORT') or (sys.argv[1] if len(sys.argv) > 1 else PORT))
    bind = '0.0.0.0' if os.environ.get('PORT') else '127.0.0.1'
    async with websockets.serve(handler, bind, port, process_request=health):
        print('Scramble Arena signalling server listening on %s:%d' % (bind, port), flush=True)
        await asyncio.Future()


if __name__ == '__main__':
    asyncio.run(main())

