const { algorithms, ChecksumError } = require('../../api/apiUtils/integrity/validateChecksums');
const { Transform } = require('stream');

class ChecksumTransform extends Transform {
    constructor(algoName, expectedDigest, isTrailer) {
        super({});
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
            // FIXME: Handle trailer is missing

            const expected = this.trailerChecksumValue;
            if (!this.algo.isValidDigest(expected)) {
                return { error: ChecksumError.MalformedChecksum, details: { algorithm: this.algoName, expected } };
            }

            // Trailer mismatch
            if (this.trailerChecksumName !== `x-amz-checksum-${this.algoName}`) {
                return { error: ChecksumError.AlgoNotSupportedSDK, details: { algorithm: this.algoName } };
            }

            if (this.digest !== this.trailerChecksumValue) {
                return {
                    error: ChecksumError.XAmzMismatch,
                    details: { algorithm: this.algoName, calculated: this.digest, expected },
                };
            }

            return null;
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
        Promise.resolve(this.algo.digestFromHash(this.hash))
            .then(digest => {
                this.digest = digest;
                return callback();
            })
            .catch(callback);
    }

    _transform(chunk, encoding, callback) {
        const input = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        // console.log("chunk: '%s'", input);
        this.hash.update(input, encoding);
        callback(null, input, encoding);
    }
}

module.exports = ChecksumTransform;
