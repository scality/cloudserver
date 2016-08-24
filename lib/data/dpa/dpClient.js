const assert = require('assert');
const async = require('async');
const PassThrough = require('stream').PassThrough;

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

        this.encode(stream, (err, fragments) => {
            if (err) {
                return callback(err);
            }

            const keys = new Array(fragments.length);
            async.forEachOf(fragments, (frag, idx, cb) => {
                const streamFrag = new PassThrough;
                streamFrag.end(frag);

                client.put(streamFrag, frag.length, params, reqUids,
                    (err, key) => {
                        if (err) {
                            log.error('error from datastore',
                                { error: err, implName });
                            return cb(err);
                        }
                        keys[idx] = key;
                        return cb();
                    });
            }, err => {
                if (err) {
                    log.error('error from datastore', { error: err, implName });
                    // delete stored fragments
                    const storedKeys = keys.filter(key =>
                        key !== undefined).join(',');
                    if (storedKeys === '') {
                        return callback(err);
                    }
                    return this.delete(storedKeys, reqUids, err1 => {
                        if (err1) {
                            process.stderr.write('Failed to clean put');
                        }
                        return callback(err);
                    });
                }
                return callback(null, keys.join(','));
            });
            return undefined;
        });
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
        assert.strictEqual(typeof key, 'string');
        const log = this.createLogger(reqUids);

        const fragments = [];
        let recDataFragsNb = 0;
        let decodeStarted = false;
        let startDecoding = false;
        key.split(',').forEach((keyFrag, fragIdx) => {
            client.get(keyFrag, null, reqUids, (err, val) => {
                if (err) {
                    log.error('error from dpClient get', { error: err });
                    return callback(err);
                }
                const buf = [];
                val.on('data', buffer => {
                    buf.push(buffer);
                });
                val.on('end', () => {
                    const dataBuf = Buffer.concat(buf);
                    fragments.push(dataBuf);
                    if (fragIdx < this.ecParams.k) {
                        recDataFragsNb++;
                    }
                    startDecoding = (recDataFragsNb === this.ecParams.k ||
                                     fragments.length === this.kmin);
                    if (startDecoding && !decodeStarted) {
                        decodeStarted = true;
                        this.decode(fragments, 0, (err, obj) => {
                            if (err) {
                                log.error('error from dpClient decode',
                                    { error: err });
                                return callback(err);
                            }
                            const stream = new PassThrough;
                            if (range && range.length === 2) {
                                stream.end(obj.slice(range[0], range[1] + 1));
                            } else {
                                stream.end(obj);
                            }
                            return callback(null, stream);
                        });
                    }
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
        assert.strictEqual(typeof key, 'string');
        const log = this.createLogger(reqUids);

        async.forEachOf(key.split(','), (keyFrag, idx, cb) => {
            client.delete(keyFrag, reqUids, err => {
                if (err) {
                    log.error('error from dpClient delete', { error: err });
                }
                return cb(err);
            });
        }, err => {
            if (err) {
                return callback(err);
            }
            return callback();
        });
    }

    /**
     * This encodes object
     * @param {http.IncomingMessage} stream - Request with the data to send
     * @param {string} stream.contentHash - hash of the data to send
     * @param {callback} cb - callback
     * @returns {undefined}
     */
    encode(stream, cb) {
        const buf = [];
        stream.on('data', buffer => {
            buf.push(buffer);
        });
        stream.on('end', () => {
            const dataBuf = Buffer.concat(buf);
            this.ec.encode(dataBuf, (err, dataArr, parityArr, len) => {
                if (err) {
                    return cb(err);
                }
                const fragments = dataArr.concat(parityArr);
                return cb(err, fragments, len);
            });
        });
    }

    /**
     * This decodes object
     * @param {Array} fragments - Array of fragments
     * @param {Boolean} metadataCheck - Checking of the metadata
     * @param {callback} cb - callback
     * @returns {undefined}
     */
    decode(fragments, metadataCheck, cb) {
        return this.ec.decode(fragments, metadataCheck, cb);
    }
}

module.exports = DpClient;
