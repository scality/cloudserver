const { errors } = require('arsenal');
const { algorithms, ChecksumError } = require('../../api/apiUtils/integrity/validateChecksums');
const { Transform } = require('stream');

class ChecksumTransform extends Transform {
    constructor(algoName, expectedDigest, isTrailer, log) {
        super({});
        this.log = log;
        this.algoName = algoName;
        this.algo = algorithms[algoName];
        this.hash = this.algo.createHash();
        this.digest = undefined;
        this.expectedDigest = expectedDigest;
        this.isTrailer = isTrailer;
        this.trailerChecksumName = undefined;
        this.trailerChecksumValue = undefined;
    }

    setExpectedChecksum(name, value) {
        this.trailerChecksumName = name;
        this.trailerChecksumValue = value;
    }

    validateChecksum() {
        if (this.isTrailer) {
            // x-amz-trailer in headers but no trailer in body.
            if (this.trailerChecksumValue === undefined) {
                return {
                    error: ChecksumError.TrailerMissing,
                    details: { expectedTrailer: `x-amz-checksum-${this.algoName}` },
                };
            }

            if (this.trailerChecksumName !== `x-amz-checksum-${this.algoName}`) {
                return { error: ChecksumError.TrailerAlgoMismatch, details: { algorithm: this.algoName } };
            }

            const expected = this.trailerChecksumValue;
            if (!this.algo.isValidDigest(expected)) {
                return {
                    error: ChecksumError.TrailerChecksumMalformed,
                    details: { algorithm: this.algoName, expected },
                };
            }

            if (this.digest !== this.trailerChecksumValue) {
                return {
                    error: ChecksumError.XAmzMismatch,
                    details: { algorithm: this.algoName, calculated: this.digest, expected },
                };
            }

            return null;
        }

        if (this.trailerChecksumValue !== undefined) {
            // Trailer found in the body but no x-amz-trailer in the headers.
            return {
                error: ChecksumError.TrailerUnexpected,
                details: { name: this.trailerChecksumName, val: this.trailerChecksumValue },
            };
        }

        if (this.expectedDigest) {
            if (this.digest !== this.expectedDigest) {
                return {
                    error: ChecksumError.XAmzMismatch,
                    details: { algorithm: this.algoName, calculated: this.digest, expected: this.expectedDigest },
                };
            }
        }

        return null;
    }

    _flush(callback) {
        Promise.resolve(this.algo.digestFromHash(this.hash)).then(
            digest => {
                this.digest = digest;
                callback();
            },
            err => {
                this.log.error('failed to compute checksum digest', { error: err, algorithm: this.algoName });
                callback(errors.InternalError);
            },
        );
    }

    _transform(chunk, encoding, callback) {
        const input = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        this.hash.update(input);
        callback(null, input);
    }
}

module.exports = ChecksumTransform;
