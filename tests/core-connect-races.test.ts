import { expect, it, vi } from 'vitest';
import net from 'node:net';
import { setTimeout as delay } from 'node:timers/promises';
import { CutProxy } from '../src/cut-proxy.js';

const query = Buffer.from([0x51, 0, 0, 0, 11, ...Buffer.from('COMMIT\0')]);
const startup = Buffer.from([0, 0, 0, 8, 0, 3, 0, 0]);

it('reserves a cut synchronously while upstream DNS/connect is pending', async () => {
  const peers = new Set<net.Socket>();
  let received = Buffer.alloc(0);
  const server = net.createServer(socket => {
    peers.add(socket);
    socket.on('error', () => {});
    socket.on('data', data => { received = Buffer.concat([received, data]); });
  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  const connect = net.connect;
  let release: (() => void) | undefined;
  const spy = vi.spyOn(net, 'connect').mockImplementation(((...args: unknown[]) => {
    if (args[1] === 'held.test') {
      return connect({
        port: args[0] as number,
        host: 'held.test',
        autoSelectFamily: false,
        lookup(_hostname, _options, callback) {
          release = () => callback(null, '127.0.0.1', 4);
        },
      });
    }
    return Reflect.apply(connect, net, args);
  }) as typeof net.connect);
  const proxy = await CutProxy.start({
    mode: 'after_forward', upstreamHost: 'held.test',
    upstreamPort: (server.address() as net.AddressInfo).port,
  });
  const client = connect(proxy.port, '127.0.0.1');
  client.on('error', () => {});
  try {
    await new Promise<void>(resolve => client.once('connect', resolve));
    client.write(Buffer.concat([startup, query]));
    for (let i = 0; i < 100 && proxy.cuts.length === 0; i++) await delay(5);
    expect(proxy.cuts).toHaveLength(1);
    client.write(query);
    await delay(30);
    expect(proxy.cuts).toHaveLength(1);
    release!();
    await proxy.commitCut;
    for (let i = 0; i < 100 && received.length < startup.length + query.length; i++) await delay(5);
    expect(received).toEqual(Buffer.concat([startup, query]));
    expect(proxy.cutCount).toBe(1);
  } finally {
    spy.mockRestore();
    client.destroy();
    await proxy.stop(0);
    for (const peer of peers) peer.destroy();
    await new Promise<void>(resolve => server.close(() => resolve()));
  }
});
