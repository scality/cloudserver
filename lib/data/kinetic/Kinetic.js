import crypto from 'crypto';
import EventEmitter from 'events';
import kinetic from 'kineticlib';
import stream from 'stream';
import net from 'net';
import { errors } from 'arsenal';

import config from '../../Config';

const HEADER_SZ = 9;

class Kinetic {

    constructor(drives) {
        this._events = new EventEmitter();
        this._sockets = [];
        this._drives = drives;
        for(let i = 0; i < drives.length; i++) {
            this._sockets.push(
                {sock: this._newSocket(),
                 connectionID: 0,
                 clusterVersion: 0,
                 drive: drives[i] });
        }
        this._request = false;
        this._chunk = new Buffer(0);
        this._chunkSize = 0;
        this._chunkTab = [];
        this._count = 0;
        this._socketPick;
        this.connect();
        return this;
    }

    connect() {
        let index = 0;
        this._sockets.forEach(socket => {
            socket.sock.connect(socket.drive, () => {
                this._bindReadable(socket, index);
                ++index;
            });
            i++;
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
        const protobufSize = header.readInt32BE(1);
        const rawData = socket.sock.read(protobufSize);
        const pdu = new kinetic.PDU(Buffer.concat([header, rawData]));
        const err = this._propError(pdu);
        return callback(err, pdu);
    }

    _parseRequest(socket, err, pdu, index) {
        const sock = socket;
        switch (pdu.getMessageType()) {
        case null:
            this._initPDU = pdu;
            sock.connectionID = pdu.getConnectionId();
            sock.clusterVersion = pdu.getClusterVersion();
            sock.index = index;
            sock.sequence = 0;
            return pdu;
            break;
        case kinetic.ops.PUT_RESPONSE:
            this._events.emit('putResponse', err);
            break;
        case kinetic.ops.DELETE_RESPONSE:
            this._events.emit('deleteResponse', err);
            break;
        case kinetic.ops.GETLOG_RESPONSE:
            this._events.emit('getLogResponse', err, socket, pdu.getLogObject());
            break;
        case kinetic.ops.GET_RESPONSE:
            if (err) {
                this._events.emit('getResponse', err);
            }
            this._startGet(socket, err, pdu);
            break;
        default:
            break;
        }
    }

    _startGet(socket, err, pdu) {
        let chunk = new Buffer(0);
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
        let chunk = new Buffer(0);
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
        if (statusCode !== kinetic.errors.SUCCESS) {
            if (statusCode === kinetic.errors.NOT_FOUND) {
                return errors.ObjNotFound;
            }
            return pdu.getErrorMessage();
        }
        return undefined;
    }

    getConnectionId() {
        return this._connectionId;
    }

    getDrives(){
        return this._drives;
    }

    getPDU(){
        return this._initPDU;
    }

    getSocket() {
        return this._socket;
    }

    setSequence(index, sequence) {
        if (sequence >= Number.MAX_VALUE) {
            const temp = this._sockets[index];
            this._sockets[index] = this._newSocket();
            this.connect();
            temp.destroy();
            this._sockets[index].sequence = 0;
        } else {
            this._sockets[index].sequence = sequence;
        }
        return this;
    }

    put(value, options, callback) {
        this._getDrive((err, socket) => {
            if (err) {
                return callback(err);
            }
            const key = Buffer.concat(
                [Buffer.from(`${socket.drive.host}:`), crypto.randomBytes(9)]);
            const obj = Buffer.concat(value);
            const tag = crypto.createHmac('sha1', 'asdfasdf').update(obj)
                      .digest();
            const pdu = new kinetic.PutPDU(
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
        })
    }

    _getDrive(callback) {
        this._bindGetLog(callback);
        this._sockets.forEach(socket => {
            const pdu = new kinetic.GetLogPDU(
                socket.sequence,
                socket.connectionID,
                socket.clusterVersion,
                { types: [kinetic.logs.CAPACITIES] });
            const header = pdu.read();
            this.setSequence(socket.index, socket.sequence + 1);
            socket.sock.write(header, err => {
                if (err) {
                    return callback(err);
                }
                return undefined;
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
                this._socketPick = {}
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
        });
    }

    get(key, range, reqUids, callback) {
        const host = key.toString().split(':')[0];
        this._chooseHost(host, socket => {
            const pdu = new kinetic.GetPDU(
                socket.sequence, socket.connectionID,
                socket.clusterVersion, key);
            this.setSequence(socket.index, socket.sequence + 1);
            const header = pdu.read();
            socket.sock.write(header, err => {
                if (err) {
                    return callback(err);
                }
                this._events.once('getResponse', (err, chunk) => {
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

    delete(key, callback) {
        const host = key.toString().split(':')[0];
        this._chooseHost(host, socket => {
            const pdu = new kinetic.DeletePDU(
                socket.sequence, socket.connectionID,
                socket.clusterVersion, key);
            this.setSequence(socket.index, socket.sequence + 1);
            socket.sock.write(pdu.read(), err => {
                if (err) {
                    return callback(err);
                }
                this._events.removeAllListeners('deleteResponse');
                this._events.once('deleteResponse', err => {
                    return callback(err);
                });
            });
        });
    }

    _chooseHost(host, callback) {
        this._sockets.forEach(socket => {
            if (socket.drive.host === host) {
                return callback(socket);
            }
        });
    }
}

export default Kinetic;
