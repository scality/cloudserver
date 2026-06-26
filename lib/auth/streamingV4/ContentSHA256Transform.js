const crypto = require('crypto');
const { Transform } = require('stream');
const { ChecksumError } = require('../../api/apiUtils/integrity/validateChecksums');

/**
 * Computes the sha256 of the streamed body to verify it against a literal
 * x-amz-content-sha256 header (the SigV4 payload hash) via validateChecksum().
 */
class ContentSHA256Transform extends Transform {
    constructor(expectedDigest, log) {
        super({});
        this.log = log;
        this.expectedDigest = (expectedDigest || '').toLowerCase();
        this.hash = crypto.createHash('sha256');
        this.digest = undefined;
    }

    validateChecksum() {
        if (this.digest !== this.expectedDigest) {
            return {
                error: ChecksumError.ContentSHA256Mismatch,
                details: { calculated: this.digest, expected: this.expectedDigest },
            };
        }
        return null;
    }

    _transform(chunk, encoding, callback) {
        const input = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        this.hash.update(input);
        callback(null, input);
    }

    _flush(callback) {
        this.digest = this.hash.digest('hex');
        callback();
    }
}

module.exports = ContentSHA256Transform;
