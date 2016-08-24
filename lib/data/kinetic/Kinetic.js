import crypto from 'crypto';
import EventEmitter from 'events';
import net from 'net';
import stream from 'stream';

import { errors } from 'arsenal';
import kineticlib from 'kineticlib';

const HEADER_SZ = 9;

class Kinetic {

    /**
     * @constructor
     * @param {Object.<string>[]} drives - the list of drives
     * @returns {Kinetic} - to allow for a functional style.
     */
    constructor(drives) {
        this._events = new EventEmitter();
        this._sockets = [];
        this._drives = drives;
        for (let i = 0; i < drives.length; i++) {
            this._sockets.push(
                { sock: this._newSocket(),
                  connectionID: 0,
                  clusterVersion: 0,
                  drive: drives[i] });
        }
        this._request = false;
        this._chunkSize = 0;
        this._chunkTab = [];
        this._count = 0;
        this.connect();
        return this;
    }

    /**
     * Connect the sockets to the drives
     * @return {undefined}
     */
    connect() {
        let index = 0;
        this._sockets.forEach(socket => {
            socket.sock.on('connect', () => {
                this._bindReadable(socket, index);
                ++index;
            });
            socket.sock.connect(socket.drive);
        });
    }

    _bindReadable(socket, index) {
        socket.sock.on('readable', err => {
            if (this._request === false) {
                this._parsePDU(socket, (err1, pdu) => {
                    this._parseRequest(socket, err || err1, pdu, index);
                });
            } else {
                this._endGet(socket);
            }
        });
    }

    _parsePDU(socket, callback) {
        const header = socket.sock.read(HEADER_SZ);
        if (header !== null) {
            const protobufSize = header.readInt32BE(1);
            const rawData = socket.sock.read(protobufSize);
            const pdu = new kineticlib.PDU(Buffer.concat([header, rawData]));
            const err = this._propError(pdu);
            return callback(err, pdu);
        }
        return undefined;
    }

    _parseRequest(socket, err, pdu, index) {
        const sock = socket;
        switch (pdu.getMessageType()) {
        case null:
            sock._initPDU = pdu;
            sock.connectionID = pdu.getConnectionId();
            sock.clusterVersion = pdu.getClusterVersion();
            sock.index = index;
            sock.sequence = 0;
            return pdu;
        case kineticlib.ops.PUT_RESPONSE:
            this._events.emit('putResponse', err);
            break;
        case kineticlib.ops.DELETE_RESPONSE:
            this._events.emit('deleteResponse', err);
            break;
        case kineticlib.ops.GETLOG_RESPONSE:
            this._events.emit(
                'getLogResponse', err, socket, pdu.getLogObject());
            break;
        case kineticlib.ops.GET_RESPONSE:
            if (err) {
                this._events.emit('getResponse', err);
            } else {
                this._startGet(socket, pdu);
            }
            break;
        default:
            break;
        }
        return undefined;
    }

    _startGet(socket, pdu) {
        let chunk = Buffer.allocUnsafe(0);
        this._request = true;
        this._chunkSize = pdu.getChunkSize();
        chunk = socket.sock.read();
        this._count += chunk.length;
        if (this._count === this._chunkSize) {
            this._events.emit('getResponse', null, chunk);
        } else {
            this._chunkTab.push(chunk);
        }
    }

    _endGet(socket) {
        let chunk = Buffer.allocUnsafe(0);
        if (this._count !== this._chunkSize) {
            chunk = socket.sock.read();
            this._chunkTab.push(chunk);
            this._count += chunk.length;
        }
        if (this._count === this._chunkSize) {
            this._events.emit(
                'getResponse', null, Buffer.concat(this._chunkTab));
        }
    }

    _newSocket() {
        const socket = new net.Socket({ allowHalfOpen: false }).pause();
        socket.setKeepAlive(true);
        socket.unref();
        return socket;
    }

    _propError(pdu) {
        const statusCode = pdu.getStatusCode();
        if (statusCode === kineticlib.errors.NOT_FOUND) {
            return errors.ObjNotFound;
        }
        if (statusCode !== kineticlib.errors.SUCCESS) {
            return pdu.getErrorMessage();
        }
        return undefined;
    }

    /**
     * set the sequence of the request.
     *
     * @param {number} index - the index of the socket.
     * @param {number} sequence - the sequence to be set.
     * @returns {Kinetic} - To allow for a functional style.
     */
    setSequence(index, sequence) {
        if (sequence >= Number.MAX_VALUE) {
            const temp = this._sockets[index].sock;
            this._sockets[index].sock = this._newSocket();
            this._sockets[index].sock.on('connect', () => {
                this._bindReadable(this._sockets[index], index);
            });
            this._sockets[index].sock.connect(this._sockets[index].drive);
            temp.destroy();
            this._sockets[index].sequence = 0;
        } else {
            this._sockets[index].sequence = sequence;
        }
        return this;
    }

    getSockets() {
        return this._sockets;
    }

    /**
     * send the put request to the right drive.
     *
     * @param {Buffer} value - the value to be put.
     * @param {Object} options - the options object for the put request:
     *                     kinetic-protocol
     *        {string} options.synchronization - synchronization mode
     *        {boolean} options.force - option for forcing the put
     * @param {function} callback - the callback
     * @returns {undefined}.
     */
    put(value, options, callback) {
        this._getDrive((err, socket) => {
            if (err) {
                return callback(err);
            }
            const key = Buffer.concat(
                [Buffer.from(`${socket.drive.host}:${socket.drive.port}`),
                 crypto.randomBytes(6)]);
            const obj = Buffer.concat(value);
            const tag = crypto.createHmac('sha1', 'asdfasdf').update(obj)
                      .digest();
            const pdu = new kineticlib.PutPDU(
                socket.sequence, socket.connectionID, socket.clusterVersion,
                key, obj.length, tag, options);
            this.setSequence(socket.index, socket.sequence + 1);
            const header = pdu.read();
            const len = header.length + obj.length;
            return socket.sock.write(Buffer.concat([header, obj], len), err => {
                if (err) {
                    return callback(err);
                }
                return this._events.once(
                    'putResponse', err => callback(err, key));
            });
        });
    }

    _getDrive(callback) {
        this._sockets.forEach(socket => {
            const pdu = new kineticlib.GetLogPDU(
                socket.sequence,
                socket.connectionID,
                socket.clusterVersion,
                { types: [kineticlib.logs.CAPACITIES] });
            const header = pdu.read();
            this.setSequence(socket.index, socket.sequence + 1);
            socket.sock.write(header, err => {
                if (err) {
                    return callback(err);
                }
                return this._bindGetLog(callback);
            });
        });
    }

    _getCapacity(logs) {
        return logs.capacity.portionFull;
    }

    _bindGetLog(callback) {
        let count = 0;
        this._events.removeAllListeners('getLogResponse');
        this._events.on('getLogResponse', (err, sock, logs) => {
            const capacities = this._getCapacity(logs);
            if (!this._socketPick) {
                this._socketPick = {};
                this._socketPick.cap = capacities;
                this._socketPick.sock = sock;
            }
            if (capacities < this._socketPick.cap) {
                this._socketPick.cap = capacities;
                this._socketPick.sock = sock;
            }
            count++;
            if (count === this._sockets.length) {
                return callback(err, this._socketPick.sock);
            }
            return undefined;
        });
    }

    /**
     * send the get request to the right drive.
     *
     * @param {Buffer} key - the value to be put.
     * @param {number[]} range - the range to get
     * @param {function} callback - the callback
     * @returns {undefined}.
     */
    get(key, range, callback) {
        const keyTab = key.toString().split(':');
        const host = keyTab[0];
        const port = keyTab[1];
        this._chooseHost(host, port, socket => {
            const pdu = new kineticlib.GetPDU(
                socket.sequence, socket.connectionID,
                socket.clusterVersion, key);
            this.setSequence(socket.index, socket.sequence + 1);
            const header = pdu.read();
            socket.sock.write(header, err => {
                if (err) {
                    return callback(err);
                }
                return this._events.once('getResponse', (err, chunk) => {
                    this._request = false;
                    this._chunkTab = [];
                    this._count = 0;
                    return callback(err, new stream.Readable({
                        read() {
                            this.push(chunk);
                            this.push(null);
                        },
                    }));
                });
            });
        });
    }

    /**
     * send the delete request to the right drive.
     *
     * @param {Buffer} key - the value to be put.
     * @param {function} callback - the callback
     * @returns {undefined}.
     */
    delete(key, callback) {
        const keyTab = key.toString().split(':');
        const host = keyTab[0];
        const port = keyTab[1];
        this._chooseHost(host, port, socket => {
            const pdu = new kineticlib.DeletePDU(
                socket.sequence, socket.connectionID,
                socket.clusterVersion, key);
            this.setSequence(socket.index, socket.sequence + 1);
            socket.sock.write(pdu.read(), err => {
                if (err) {
                    return callback(err);
                }
                this._events.removeAllListeners('deleteResponse');
                return this._events.once(
                    'deleteResponse', err => callback(err));
            });
        });
    }

    _chooseHost(host, port, callback) {
        this._sockets.forEach(socket => {
            if (socket.drive.host === host
                && socket.drive.port) {
                return callback(socket);
            }
            return undefined;
        });
    }
}

export default Kinetic;
