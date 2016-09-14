'use strict'; // eslint-disable-line strict

const assert = require('assert');
const Readable = require('stream').Readable;

const Logger = require('werelogs').Logger;
const EcLib = require('eclib');

const config = require('../../Config').default;

const file = require('../file/backend').default;
const inMemory = require('../in_memory/backend').default;
const Sproxy = require('sproxydclient');

let client;
let implName;

if (config.backends.data === 'mem') {
    client = inMemory;
    implName = 'mem';
} else if (config.backends.data === 'file') {
    client = file;
    implName = 'file';
} else if (config.backends.data === 'scality') {
    client = new Sproxy({
        bootstrap: config.sproxyd.bootstrap,
        log: config.log,
        chordCos: config.sproxyd.chordCos,
    });
    implName = 'sproxyd';
}

const backendId = {
    EC_BACKEND_NULL: 0,
    EC_BACKEND_JERASURE_RS_VAND: 1,
    EC_BACKEND_JERASURE_RS_CAUCHY: 2,
    EC_BACKEND_FLAT_XOR_HD: 3,
    EC_BACKEND_ISA_L_RS_VAND: 4,
    EC_BACKEND_SHSS: 5,
    EC_BACKEND_LIBERASURECODE_RS_VAND: 6,
    EC_BACKENDS_MAX: 99,
};

const checksumType = {
    CHKSUM_NONE: 1,
    CHKSUM_CRC32: 2,
    CHKSUM_MD5: 3,
    CHKSUM_TYPES_MAX: 99,
};

const ecDefault = {
    bc_id: backendId.EC_BACKEND_JERASURE_RS_VAND, // eslint-disable-line
    k: 9,
    m: 1,
    w: 8,
    hd: 2,
    ct: checksumType.CHKSUM_CRC32,
};

const placementPolicy = {
    default: 'all',
};

const CHUNK_SIZE = 64 * 1024; // 64KB
/*
 * create a readable stream from a buffer
 */
function createReadbleStream(buf, size) {
    const chunkSize = Math.min(CHUNK_SIZE, size);
    let start = 0;
    return new Readable({
        read: function read() {
            while (start < size) {
                const finish = Math.min(start + chunkSize, size);
                this.push(buf.slice(start, finish));
                start += chunkSize;
            }
            if (start >= size) {
                this.push(null);
            }
        },
    });
}


class DpClient {
    /**
     * This represent our interface with the object client.
     * @constructor
     * @constructor
     * @param {Object} [opts] - Contains options used by the library
     * @param {String} [opts.pp] - placement policy
     * @param {Object} [opts.ec] - erasure codes parameters
     * @param {Number} [opts.ec.bc_id=0] - Backend ID
     * @param {Number} [opts.ec.k=8] - Number of data fragments
     * @param {Number} [opts.ec.m=4] - Number of parity fragments
     * @param {Number} [opts.ec.w=0] - word size (in bits)
     * @param {Number} [opts.ec.hd=0] - hamming distance (== m for Reed-Solomon)
     * @param {Number} [opts.ec.ct=0] - checksum type
     */
    constructor(opts) {
        const options = opts || {};
        this.pp = options.pp || placementPolicy.default;
        this.ecParams = options.ec || ecDefault;
        this.kmin = this.ecParams.k + this.ecParams.m - this.ecParams.hd + 1;
        this.ecParamsStr = JSON.stringify(this.ecParams);
        this.ec = new EcLib(this.ecParams);
        this.ec.init();

        this.fragHeaderSize = this.ec.getHeaderSize();
        this.zeroHeader = new Buffer(this.fragHeaderSize).fill(0);

        this.setupLogging(options.log);
    }

    setupLogging(config) {
        let options = undefined;
        if (config !== undefined) {
            options = {
                level: config.logLevel,
                dump: config.dumpLevel,
            };
        }
        this.logging = new Logger('DpClient', options);
    }

    createLogger(reqUids) {
        return reqUids ?
            this.logging.newRequestLoggerFromSerializedUids(reqUids) :
            this.logging.newRequestLogger();
    }

    /**
     * This sends a PUT request to object client.
     * @param {http.IncomingMessage} stream - Request with the data to send
     * @param {number} size - data size in the stream
     * @param {Object} params - parameters for key generation
     * @param {String} reqUids - The serialized request id
     * @param {Callback} callback - callback
     * @returns {undefined}
     */
    put(stream, size, params, reqUids, callback) {
        assert(stream.readable, 'stream should be readable');
        const log = this.createLogger(reqUids);
        if (size === 0) {
            return client.put(stream, size, params, reqUids, callback);
        }

        const codeLen = this.ecParams.k + this.ecParams.m;
        const keys = new Array(codeLen);
        const alignedSize = this.ec.getAlignedDataSize(size);
        const fragSize = alignedSize / this.ecParams.k;
        const fullFragSize = fragSize + this.fragHeaderSize;
        let noError = true;

        const object = new Buffer(alignedSize);
        let cursor = 0;
        let fragCursor = 0;
        let dataLen = 0;

        let donesNb = 0;
        let donesAll = 0;
        stream.on('data', chunk => {
            chunk.copy(object, cursor);
            const cSize = chunk.length;
            cursor += cSize;
            dataLen += cSize;
            if (dataLen >= fragSize) {
                const fragsNb = Math.floor(dataLen / fragSize);
                const len = fragsNb * fragSize;
                const start = fragCursor * fragSize;
                const buf = object.slice(start, start + len);
                dataLen -= len;

                const startIdx = fragCursor;
                fragCursor += fragsNb;

                this.streamData(buf, len, startIdx, fragSize, size, keys,
                    params, log, (err, keysNb) => {
                        if (err) {
                            noError = false;
                            log.error('error from datastore',
                                { error: err, implName });
                            return undefined;
                        }
                        donesNb += keysNb;
                        if (donesNb === codeLen) {
                            return callback(null, keys.join(','));
                        }
                        return undefined;
                    });
            }
        });
        stream.on('error', err => {
            log.error('error from datastore',
                { error: err, implName });
            return this.cleanFrags(err, keys, log, callback);
        });
        stream.on('end', () => {
            if (!noError) {
                return this.cleanFrags('error from datastore', keys, log,
                    callback);
            }
            noError = true;
            // last data fragments
            if (dataLen > 0 || fragCursor < this.ecParams.k) {
                object.fill(0, cursor);
                const start = fragCursor * fragSize;
                const buf = object.slice(start);
                this.streamData(buf, alignedSize - start, fragCursor, fragSize,
                    size, keys, params, log, (err, keysNb) => {
                        donesAll++;
                        if (err) {
                            noError = false;
                            log.error('error from datastore',
                                { error: err, implName });
                            if (donesAll === 2) {
                                return this.cleanFrags('error from datastore',
                                    keys, log, callback);
                            }
                            return undefined;
                        }
                        donesNb += keysNb;
                        if (donesNb === codeLen) {
                            return callback(null, keys.join(','));
                        }
                        return undefined;
                    });
            } else {
                donesAll++;
            }
            // generate parity fragments
            this.ec.encode(object, (err, dataArr, parityArr) => {
                if (err) {
                    noError = false;
                    log.error('error from datastore',
                        { error: err, implName });
                    return undefined;
                }
                let count = 0;
                return parityArr.forEach((frag, idx) => {
                    this.streamFrag(frag, this.ecParams.k + idx, fullFragSize,
                        keys, params, log, err => {
                            count++;
                            if (count === this.ecParams.m) {
                                donesAll++;
                            }
                            if (err) {
                                noError = false;
                                log.error('error from datastore',
                                    { error: err, implName });
                                if (donesAll === 2) {
                                    return this.cleanFrags(
                                        'error from datastore', keys, log,
                                        callback);
                                }
                                return undefined;
                            }
                            donesNb++;
                            if (donesNb === codeLen) {
                                return callback(null, keys.join(','));
                            }
                            return undefined;
                        });
                });
            });
            return undefined;
        });
        return undefined;
    }

    /**
     * This sends a GET request to object client.
     * @param {String} key - The key associated to the value
     * @param { Number [] | Undefined} range - range (if any) with first
     * element the start and the second element the end
     * @param {String} reqUids - The serialized request id
     * @param {Callback} callback - callback
     * @returns {undefined}
     */
    get(key, range, reqUids, callback) {
        if (typeof key !== 'string') {
            key = key.toString(); // eslint-disable-line
        }
        const log = this.createLogger(reqUids);

        const fragments = [];
        let failedFragsNb = 0;
        let recDataFragsNb = 0;
        let decodeStarted = false;
        let startDecoding = false;
        key.split(',').forEach((keyFrag, fragIdx) => {
            client.get(keyFrag, null, reqUids, (err, val) => {
                if (err) {
                    log.error('error from dpClient get', { error: err });
                    failedFragsNb++;
                    if (failedFragsNb === this.ecParams.m + 1) {
                        return callback(err);
                    }
                    return undefined;
                }
                const buf = [];
                val.on('data', buffer => {
                    buf.push(buffer);
                });
                val.on('error', err => {
                    log.error('error from dpClient get', { error: err });
                    failedFragsNb++;
                    if (failedFragsNb === this.ecParams.m + 1) {
                        return callback(err);
                    }
                    return undefined;
                });
                val.on('end', () => {
                    const dataBuf = Buffer.concat(buf);

                    if (dataBuf.length === 0) {
                        return callback(null,
                            createReadbleStream(new Buffer(0), 0));
                    }

                    fragments.push(dataBuf);

                    if (fragIdx < this.ecParams.k) {
                        recDataFragsNb++;
                    }
                    startDecoding = (recDataFragsNb === this.ecParams.k ||
                                     fragments.length === this.kmin);
                    if (startDecoding && !decodeStarted) {
                        decodeStarted = true;
                        this.ec.decode(fragments, 0, (err, obj) => {
                            if (err) {
                                log.error('error from dpClient decode',
                                    { error: err });
                                return callback(err);
                            }

                            let stream;
                            if (range && range.length === 2) {
                                stream = createReadbleStream(
                                    obj.slice(range[0], range[1] + 1),
                                    range[1] - range[0] + 1);
                            } else {
                                stream = createReadbleStream(obj, obj.length);
                            }
                            return callback(null, stream);
                        });
                    }
                    return undefined;
                });

                return undefined;
            });
        });
    }

    /**
     * This sends a DELETE request to object client.
     * @param {String} key - The key associated to the value
     * @param {String} reqUids - Serialized request ID
     * @param {Callback} callback - callback
     * @returns {undefined}
     */
    delete(key, reqUids, callback) {
        if (typeof key !== 'string') {
            key = key.toString(); // eslint-disable-line
        }
        let noError = true;

        const log = this.createLogger(reqUids);

        const keys = key.split(',');
        const len = keys.length;
        let idx = 0;
        keys.forEach(keyFrag => {
            client.delete(keyFrag, reqUids, err => {
                if (err) {
                    log.error('error from dpClient delete', { error: err });
                    if (noError) {
                        noError = false;
                        return callback(err);
                    }
                }
                idx++;
                if (idx === len && noError) {
                    return callback();
                }
                return undefined;
            });
        });
    }

    /**
     * Stream fragment
     * @param{buffer} frag - fragment to be stored
     * @param{number} fragIdx - index of fragment
     * @param{number} fragSize - full size of fragment (header included)
     * @param{array} keys - array of fragments' key
     * @param{object} params - parameters of object
     * @param{object} log - logger
     * @param{callback} callback - callback(err)
     * @return{this} this
     */
    streamFrag(frag, fragIdx, fragSize, keys, params, log, callback) {
        const stream = createReadbleStream(frag, fragSize);

        log.debug(`Streaming fragment ${fragIdx}\n`);
        client.put(stream, fragSize, params, log.getSerializedUids(),
            (err, key) => {
                if (err) {
                    log.error('error from datastore',
                        { error: err, implName });
                    return callback(err);
                }
                log.debug(`Fragment ${fragIdx} is stored\n`);
                keys[fragIdx] = key;                // eslint-disable-line
                return callback();
            });
    }

    /**
     * split a buffer to multiple fragments then store them
     * supposing len is multiple of fragSize
     * @param{buffer} buf - buffer to store
     * @param{number} len - buffer size
     * @param{number} start - starting index of fragments
     * @param{number} fragSize - data fragment size
     * @param{number} objSize - object size
     * @param{array} keys - array of fragments' key
     * @param{object} params - parameters of object
     * @param{object} log - logger
     * @param{callback} callback - callback(err, fragsNb)
     * @return{this} this
     */
    streamData(buf, len, start, fragSize, objSize, keys, params, log,
        callback) {
        const fragsNb = len / fragSize;
        let cursor = 0;
        let donesNb = 0;

        function cb(err) {
            if (err) {
                return callback(err);
            }
            donesNb++;
            if (donesNb === fragsNb) {
                return callback(null, fragsNb);
            }
            return undefined;
        }

        for (let idx = 0; idx < fragsNb; idx++, cursor += fragSize) {
            const fragIdx = start + idx;
            const frag = Buffer.concat([
                new Buffer(this.zeroHeader),
                buf.slice(cursor, cursor + fragSize),
            ]);
            // update header for fragments
            this.ec.addFragmentHeader(frag, fragIdx, objSize, fragSize);
            log.debug(`Added header for fragment ${fragIdx}\n`);
            this.streamFrag(frag, fragIdx, fragSize + this.fragHeaderSize,
                keys, params, log, cb);
        }
    }

    /**
     * Delete stored fragments
     * @param{object} error - error to be return
     * @param{array} keys - array of fragments' key
     * @param{object} log - logger
     * @param{callback} callback - callback(error)
     * @return{this} this
     */
    cleanFrags(error, keys, log, callback) {
        const storedKeys = keys.filter(key => key !== undefined).join(',');
        if (storedKeys === '') {
            return callback(error);
        }
        return this.delete(storedKeys, log.getSerializedUids(), err => {
            if (err) {
                log.debug(`Failed to clean put ${err}`);
            }
            return callback(error);
        });
    }
}

module.exports = DpClient;
