const { errors } = require('arsenal');
const { algorithms } = require('../../api/apiUtils/integrity/validateChecksums');
const { Writable } = require('stream');

/**
 * Writable sink that hashes everything written to it and discards the bytes.
 *
 * The computed digest is available on `.digest` once the 'finish' event fires.
 */
class ChecksumWritable extends Writable {
    constructor(algoName, log) {
        super({});
        this.log = log;
        this.algoName = algoName;
        this.algo = algorithms[algoName];
        this.hash = this.algo.createHash();
        this.digest = undefined;
    }

    _write(chunk, encoding, callback) {
        const input = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        this.hash.update(input);
        callback();
    }

    _final(callback) {
        Promise.resolve(this.algo.digestFromHash(this.hash))
            .then(digest => {
                this.digest = digest;
                callback();
            }, err => {
                this.log.error('failed to compute checksum digest', { error: err, algorithm: this.algoName });
                callback(errors.InternalError);
            });
    }
}

module.exports = ChecksumWritable;
