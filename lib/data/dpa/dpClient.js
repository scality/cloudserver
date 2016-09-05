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
        const zeroHeader = new Buffer(this.fragHeaderSize).fill(0);
        let noError = true;

        const object = new Buffer(alignedSize);
        let cursor = 0;
        let fragCursor = 0;
        let dataLen = 0;

        // stream a buffer
        function streamFrag(frag, fragIdx, cb) {
            const stream = createReadbleStream(frag, fullFragSize);

            log.debug(`Streaming fragment ${fragIdx}\n`);
            client.put(stream, fullFragSize, params, reqUids, (err, key) => {
                if (err) {
                    log.error('error from datastore',
                        { error: err, implName });
                    return cb(err);
                }
                log.debug(`Fragment ${fragIdx} is stored\n`);
                keys[fragIdx] = key;
                return cb();
            });
        }

        // delete stored fragments
        function cleanFrags(error, cb) {
            const storedKeys = keys.filter(key => key !== undefined).join(',');
            if (storedKeys === '') {
                return cb(error);
            }
            return this.delete(storedKeys, reqUids, err => {
                if (err) {
                    log.debug(`Failed to clean put ${err}`);
                }
                return cb(error);
            });
        }

        // stream a buffer to fragments
        // supposing len is multiple of fragSize
        function streamData(buf, len, startIdx, cb) {
            const fragsNb = len / fragSize;
            let cursor = 0;
            let fragIdx = startIdx;
            while (cursor < len) {
                const frag = Buffer.concat([
                    new Buffer(zeroHeader),
                    buf.slice(cursor, cursor + fragSize),
                ]);
                // update header for fragments
                this.ec.addFragmentHeader(frag, fragIdx, size, fragSize);
                log.debug(`Added header for fragment ${fragIdx}\n`);
                streamFrag(frag, fragIdx, err => {
                    if (err) {
                        return cb(err);
                    }
                    return undefined;
                });
                cursor += fragSize;
                fragIdx++;
            }
            return cb(null, fragsNb);
        }

        let donesNb = 0;
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

                streamData.bind(this)(buf, len, startIdx, (err, keysNb) => {
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
            return cleanFrags.bind(this)(err, callback);
        });
        stream.on('end', () => {
            if (!noError) {
                return cleanFrags.bind(this)('error from datastore', callback);
            }
            noError = true;
            let donesAll = 0;
            // last data fragments
            if (dataLen > 0 || fragCursor < this.ecParams.k) {
                object.fill(0, cursor);
                const start = fragCursor * fragSize;
                const buf = object.slice(start);
                streamData.bind(this)(buf, alignedSize - start,
                    fragCursor, (err, keysNb) => {
                        donesAll++;
                        if (err) {
                            noError = false;
                            log.error('error from datastore',
                                { error: err, implName });
                            if (donesAll === 2) {
                                return cleanFrags.bind(this)(
                                    'error from datastore', callback);
                            }
                            return undefined;
                        }
                        donesNb += keysNb;
                        if (donesNb === codeLen) {
                            return callback(null, keys.join(','));
                        }
                        return undefined;
                    });
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
                    streamFrag(frag, this.ecParams.k + idx, err => {
                        count++;
                        if (count === this.ecParams.m) {
                            donesAll++;
                        }
                        if (err) {
                            noError = false;
                            log.error('error from datastore',
                                { error: err, implName });
                            if (donesAll === 2) {
                                return cleanFrags.bind(this)(
                                    'error from datastore', callback);
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
     * @param {String} reqUids - The serialized request id
     * @param {Callback} callback - callback
     * @returns {undefined}
     */
    delete(key, reqUids, callback) {
        if (typeof key !== 'string') {
            key = key.toString(); // eslint-disable-line
        }
        const log = this.createLogger(reqUids);
        let noError = true;

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
}

module.exports = DpClient;
