'use strict'; // eslint-disable-line strict
const assert = require('assert');
const EventEmitter = require('events');
const http = require('http');
const net = require('net');
const URL = require('url');
const WebSocket = require('ws');

const _config = require('../../../lib/Config').config;
const {
    managementAgentMessageType,
} = require('../../../lib/management/agentClient');
const {
    CHECK_BROKEN_CONNECTIONS_FREQUENCY_MS,
    WS_STATUS_IDLE,
} = require('../../../lib/management/constants');
const {
    OrbitSimulatorServer,
    OrbitSimulatorEvent,
} = require('./orbitServerSimulator');
const {
    ChannelMessageV0,
    MessageType,
} = require('../../../lib/management/ChannelMessageV0');


const PAYLOAD_MSG_TYPE = MessageType.CHANNEL_PAYLOAD_MESSAGE;
const CONFIG_OVERLAY_MSG_TYPE = MessageType.CONFIG_OVERLAY_MESSAGE;
const METRICS_REQ_MSG_TYPE = MessageType.METRICS_REQUEST_MESSAGE;
const METRICS_REPORT_MSG_TYPE = MessageType.METRICS_REPORT_MESSAGE;
const CHAN_CLOSE_MSG_TYPE = MessageType.CHANNEL_CLOSE_MESSAGE;


function testWithBrowserAccessEnabled() {
    before(done => {
        const orbitSimulator = new OrbitSimulatorServer();
        orbitSimulator.on(OrbitSimulatorEvent.EVENT_CONNECTION, () => {
            const overlay = { browserAccess: { enabled: false } };
            orbitSimulator.sendMessage(CONFIG_OVERLAY_MSG_TYPE, overlay);
            orbitSimulator.stop(done);
        });
        orbitSimulator.start();
    });

    it('should not forward payload message if no overlay has been received',
       done => {
           const chanId = 1;

           const orbitSimulator = new OrbitSimulatorServer();

           /* Payload message socket server. The management process forwards
            * payload message from Orbit. */
           const server = new net.Server();
           server.on('connection', () => {
               done(new Error('should not connect to payload message server'));
           });
           server.listen(process.env.SECURE_CHANNEL_DEFAULT_FORWARD_TO_PORT);

           /* Once the management process is connected to orbit, send it a
            * payload message. It is supposed to create the channel ID socket
            * with the payload message server. */
           orbitSimulator.on(OrbitSimulatorEvent.EVENT_CONNECTION, () => {
               orbitSimulator.sendMessage(PAYLOAD_MSG_TYPE, 'data', chanId);
               setTimeout(() => {
                   server.close(() => {
                       orbitSimulator.stop(done);
                   });
               }, 4000);
           });
           orbitSimulator.start();
       });

    /* XXX: this test set the managementProcess config browserAccessEnabled to
     * true, allowing it to forward payload message. This internal state value
     * is required for the following tests. */
    it('should close a channel ID when requested', done => {
        const chanId = 1;
        let closeMessageSent = false;

        const orbitSimulator = new OrbitSimulatorServer();

        /* Payload message socket server. The management process forwards
         * payload message from Orbit. */
        const server = new net.Server();
        server.on('connection', s => {
            s.on('close', hadError => {
                /* Make sure the connection had been closed after the CLOSE
                 * message has been sent by Orbit and there is no error. */
                assert.strictEqual(hadError, false);
                assert.strictEqual(closeMessageSent, true);

                server.close(() => { orbitSimulator.stop(done); });
            });
            s.on('data', () => {
                /* Once the payload message has been forwarded from Orbit
                 * to the socket server by the management process, make
                 * Orbit request this channel close. */
                closeMessageSent = true;
                orbitSimulator.sendMessage(CHAN_CLOSE_MSG_TYPE, '', chanId);
            });
        });
        server.listen(process.env.SECURE_CHANNEL_DEFAULT_FORWARD_TO_PORT);

        /* Once the management process is connected to orbit, send it a
         * payload message to create the channel ID with the payload
         * message server. */
        orbitSimulator.on(OrbitSimulatorEvent.EVENT_CONNECTION, () => {
            const overlay = { browserAccess: { enabled: true } };
            orbitSimulator.sendMessage(CONFIG_OVERLAY_MSG_TYPE, overlay);
            setTimeout(() => {
                orbitSimulator.sendMessage(PAYLOAD_MSG_TYPE, 'data', chanId);
            }, 1000);
        });
        orbitSimulator.start();
    });

    /* When the process management receives a payload message associated to
     * a specific channel ID from Orbit, it forwards this message  to a
     * configured host/port socket.
     * When it received data on this socket, it forwards it to Orbit. */
    it('should forward payload message', done => {
        const payload = 'payload';
        const chanId = 1;
        let socket = null;

        /* Payload message socket server. The management process forwards
         * payload message from Orbit. */
        const server = new net.Server();
        server.on('connection', s => {
            socket = s;

            /* After receiving the payload message sent by Orbit and
             * forwarded by the management process, reply on the socket. */
            socket.on('data', data => {
                assert.strictEqual(data.toString(), payload);

                socket.write(payload);
            });
        });

        /* Once the management process is connected to orbit, send it a
         * payload message. */
        const orbitSimulator = new OrbitSimulatorServer();
        orbitSimulator.on(OrbitSimulatorEvent.EVENT_CONNECTION, () => {
            orbitSimulator.sendMessage(PAYLOAD_MSG_TYPE, payload, chanId);
        });

        /* Check orbit received the message. */
        orbitSimulator.on(OrbitSimulatorEvent.EVENT_MESSAGE, data => {
            const message = new ChannelMessageV0(data);

            assert.strictEqual(message.getType(), PAYLOAD_MSG_TYPE);
            assert.strictEqual(message.payload.toString(), payload);
            assert.strictEqual(message.getChannelNumber(), chanId);

            socket.end();
            server.close(() => { orbitSimulator.stop(done); });
        });

        server.listen(process.env.SECURE_CHANNEL_DEFAULT_FORWARD_TO_PORT);
        orbitSimulator.start();
    });

    it('should use one TCP socket per channel ID', done => {
        const payload = 'payload';
        const chanId1 = 1;
        const chanId2 = 2;
        const evtEmitter = new EventEmitter();
        const clients = [];
        const payloadMsgFwdedEvent = 'payloadMessageForwarded';
        let socketServerFirstMsgReceived = false;
        let orbitFirstMsgReceived = false;

        /* Payload message socket server. The management process forwards
         * payload message from Orbit. */
        const server = new net.Server();
        server.on('connection', s => {
            assert.equal(clients.indexOf(s), -1);

            clients.push(s);
            s.on('data', () => { evtEmitter.emit(payloadMsgFwdedEvent); });
        });

        /* Once the management process is connected to orbit, send it a
         * payload message. */
        const orbitSimulator = new OrbitSimulatorServer();
        orbitSimulator.on(OrbitSimulatorEvent.EVENT_CONNECTION, () => {
            orbitSimulator.sendMessage(PAYLOAD_MSG_TYPE, payload, chanId1);
        });

        /* When the socket server receives the 1st payload, send an other
         * one with a different channel id. In both cases, reply with a
         * payload. */
        evtEmitter.on(payloadMsgFwdedEvent, () => {
            if (!socketServerFirstMsgReceived) {
                assert.equal(clients.length, 1);

                socketServerFirstMsgReceived = true;
                clients[0].write(payload);
                orbitSimulator.sendMessage(PAYLOAD_MSG_TYPE, payload,
                                           chanId2);
                return;
            }

            assert.equal(clients.length, 2);
            clients[1].write(payload);
        });

        /* Check orbit received the 2 messages. */
        orbitSimulator.on(OrbitSimulatorEvent.EVENT_MESSAGE, data => {
            const message = new ChannelMessageV0(data);

            if (!orbitFirstMsgReceived) {
                assert.strictEqual(message.getType(), PAYLOAD_MSG_TYPE);
                assert.strictEqual(message.payload.toString(), payload);
                assert.strictEqual(message.getChannelNumber(), chanId1);

                orbitFirstMsgReceived = true;
                return;
            }

            /* Ignore CHANNEL_CLOSE_MESSAGE */
            if (message.getType() !== PAYLOAD_MSG_TYPE) {
                return;
            }

            assert.strictEqual(message.payload.toString(), payload);
            assert.strictEqual(message.getChannelNumber(), chanId2);

            clients.forEach(client => { client.end(); });
            server.close(() => { orbitSimulator.stop(done); });
        });

        server.listen(process.env.SECURE_CHANNEL_DEFAULT_FORWARD_TO_PORT);
        orbitSimulator.start();
    });
}

/* Test the management process.
 *
 * This process connects to Orbit WebSocket server and is itself a WebSocket
 * server. Testing it requires:
 * - an orbit simulator,
 * - a WebSocket client to connect to it,
 * - HTTP server to answer the stats requests,
 * - a socket server for payload messages.
 **/
describe('Management process', function testSuite() {
    this.timeout(120000);

    /* Make sure the process management send payload message to a local host
     * socket to be able to receive this message in this test.  */
    assert.strictEqual(process.env.SECURE_CHANNEL_DEFAULT_FORWARD_TO_HOST,
                       'localhost');

    function createWs(path) {
        const host = _config.managementAgent.host;
        const port = _config.managementAgent.port;
        const url = `ws://${host}:${port}/${path || 'watch'}`;
        return new WebSocket(url);
    }

    it('should not listen on others routes than `watch`', done => {
        const ws = createWs('wrong_path');
        const msg = 'management agent process should not listen this route';

        ws.on('open', () => { done(new Error(msg)); });
        ws.on('unexpected-response', (_, response) => {
            assert.strictEqual(response.statusCode, 400);

            done();
        });
    });

    it('should listen on `watch` route', done => {
        const ws = createWs();

        ws.on('open', done);
        ws.on('error', error => { done(error); });
    });

    it('should terminate the connection when a client does not answer ping',
       done => {
           this.timeout(2 * CHECK_BROKEN_CONNECTIONS_FREQUENCY_MS);

           const ws = createWs();

           ws.on('close', (code, reason) => {
               assert.strictEqual(code, WS_STATUS_IDLE.code);
               assert.strictEqual(reason, WS_STATUS_IDLE.reason);

               done();
           });

           ws.on('error', error => { done(error); });

           ws.on('message', () => {
               /* Ugly eventTarget internal fields hacking to avoid this web
                * socket to answer to ping messages. It will make
                * the management agent to close the connection after a timeout.
                * Defining an onPing event does not help, this internal
                * function is still called. */
               ws._receiver._events.ping = function noop() {};
           });
       });

    it('should connect to orbit', done => {
        const orbitSimulator = new OrbitSimulatorServer();

        orbitSimulator.on(OrbitSimulatorEvent.EVENT_CONNECTION, () => {
            orbitSimulator.stop(done);
        });
        orbitSimulator.start();
    });

    it('should save the last overlay and send it to client on connection',
       done => {
           const orbitSimulator = new OrbitSimulatorServer();

           orbitSimulator.on(OrbitSimulatorEvent.EVENT_CONNECTION, () => {
               /* Send an overlay to management process. */
               const body = 'body';
               orbitSimulator.sendMessage(CONFIG_OVERLAY_MSG_TYPE, body);

               /* Connect to the process manager and check its saved
                * overlay. */
               const ws = createWs();
               ws.on('error', error => { done(new Error(error)); });
               ws.on('message', data => {
                   const msg = JSON.parse(data);
                   const type = managementAgentMessageType.NEW_OVERLAY;

                   assert.strictEqual(msg.messageType, type);
                   assert.strictEqual(msg.payload.toString(), body);

                   ws.terminate();
                   orbitSimulator.stop(done);
               });
           });
           orbitSimulator.start();
       });

    it('should send new overlay to its client', done => {
        const body = 'body';

        const orbitSimulator = new OrbitSimulatorServer();

        orbitSimulator.on(OrbitSimulatorEvent.EVENT_CONNECTION, () => {
            /* Connect to management process to receive the first overlay.
             **/
            const ws = createWs();
            let firstMsgReceived = false;

            ws.on('error', () => { done(new Error('connection error')); });
            ws.on('message', data => {
                const msg = JSON.parse(data);

                if (!firstMsgReceived) {
                    firstMsgReceived = true;
                    /* Send a new overlay to management process. */
                    orbitSimulator.sendMessage(CONFIG_OVERLAY_MSG_TYPE,
                                               body);
                    return;
                }

                /* Check we receive the second overlay. */
                assert.strictEqual(msg.payload.toString(), body);

                ws.terminate();
                orbitSimulator.stop(done);
            });
        });
        orbitSimulator.start();
    });

    it('should get and send stats to orbit on stat requests', done => {
        const stats = { stats: 'stats' };

        /* Mock stats server, it returns a JSON object stringified on
         * request on the stats path. */
        const url = URL.parse(process.env.STAT_REPORT_URL);
        const statServer = http.createServer((request, response) => {
            if (request.url !== url.pathname) {
                response.writeHead(400, { 'Content-type': 'text/plan' });
                response.write('bad path');
                response.end();
                return;
            }
            response.writeHead(200, { 'Content-type': 'text/plan' });
            response.write(JSON.stringify(stats));
            response.end();
        });
        statServer.listen(url.port);

        /* Once the management process is connected to orbit, send it a
         * stat request. */
        const orbitSimulator = new OrbitSimulatorServer();
        orbitSimulator.on(OrbitSimulatorEvent.EVENT_CONNECTION, () => {
            orbitSimulator.sendMessage(METRICS_REQ_MSG_TYPE, 'data');
        });

        /* And finally check the management process replies by a metrics
         * report message. */
        orbitSimulator.on(OrbitSimulatorEvent.EVENT_MESSAGE, data => {
            const message = new ChannelMessageV0(data);

            assert.strictEqual(message.getType(), METRICS_REPORT_MSG_TYPE);
            assert.deepStrictEqual(JSON.parse(message.payload.toString()),
                                   stats);

            statServer.close(() => { orbitSimulator.stop(done); });
        });
        orbitSimulator.start();
    });

    /* Test suite which requires the management process to have received an
     * overlay with the browserAccessEnabled set to true, otherwise payload
     * message are not forwarded. This test suite is separated from the
     * other tests because of its internal requirement. As long as there is
     * no tests launching/stoping the management process there is no
     * better way to test this. Anonymous function not used here to save an
     * indentation level. */
    describe('Management process with browser access enabled',
             testWithBrowserAccessEnabled);
});
