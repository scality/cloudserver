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
            socket.lexiKey = '0';
            return pdu;
        case kineticlib.ops.PUT_RESPONSE:
            this._events.emit('putResponse', err);
            break;
        case kineticlib.ops.DELETE_RESPONSE:
            this._events.emit('deleteResponse', err);
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
        socket._chunkSize = pdu.getChunkSize();
        if (socket._chunkSize > 0) {
            chunk = socket.sock.read();
            this._count += chunk.length;
            if (this._count === socket._chunkSize) {
                this._events.emit('getResponse', null, chunk);
            } else {
                this._chunkTab.push(chunk);
            }
        } else {
            this._events.emit('getResponse', errors.ObjNotFound);
        }
    }

    _endGet(socket) {
        let chunk = Buffer.allocUnsafe(0);
        if (this._count !== socket._chunkSize) {
            chunk = socket.sock.read();
            this._chunkTab.push(chunk);
            this._count += chunk.length;
        }
        if (this._count === socket._chunkSize) {
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
        // if (statusCode === kineticlib.errors.NOT_FOUND) {
        //     return errors.ObjNotFound;
        // }
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

    _incrString(str) {
        return (parseInt(str, 36) + 1).toString(36);
    }

    /**
     * send the put request to the right drive.
     *
     * @param {number} index - the index of the socket
     * @param {Buffer} value - the value to be put.
     * @param {Object} options - the options object for the put request:
     *                     kinetic-protocol
     *        {string} options.synchronization - synchronization mode
     *        {boolean} options.force - option for forcing the put
     * @param {function} callback - the callback
     * @returns {undefined}.
     */
    put(index, value, options, callback) {
        const socket = this._sockets[index];
        const key = Buffer.concat([Buffer.from(
            `${socket.drive.host}:${socket.drive.port}:${socket.lexiKey}`)]);
        socket.lexiKey = this._incrString(socket.lexiKey);
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
    }

    /**
     * send the get request to the right drive.
     *
     * @param {number} index - the index of the socket
     * @param {Buffer} key - the value to be put.
     * @param {number[]} range - the range to get
     * @param {function} callback - the callback
     * @returns {undefined}.
     */
    get(index, key, range, callback) {
        const socket = this._sockets[index];
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
                if (err) {
                    return callback(err);
                }
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
    }

    /**
     * send the delete request to the right drive.
     *
     * @param {number} index - the index of the socket
     * @param {Buffer} key - the value to be put.
     * @param {object} options - the options for the delete request
     *                 options.force - to force the delete
     *                 options.synchronization - the type of the delete
     * @param {function} callback - the callback
     * @returns {undefined}.
     */
    delete(index, key, options, callback) {
        const socket = this._sockets[index];
        const pdu = new kineticlib.DeletePDU(
            socket.sequence, socket.connectionID,
            socket.clusterVersion, key, options);
        this.setSequence(socket.index, socket.sequence + 1);
        socket.sock.write(pdu.read(), err => {
            if (err) {
                return callback(err);
            }
            this._events.removeAllListeners('deleteResponse');
            return this._events.once(
                'deleteResponse', err => callback(err));
        });
    }
}

export default Kinetic;
