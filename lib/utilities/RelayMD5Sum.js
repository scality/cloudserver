const crypto = require('crypto');
const { Transform } = require('stream');

/**
 * This class is designed to compute the md5 hash on multiple streams in relay
 * race style.
 */
class RelayMD5Sum extends Transform {

    /**
     * @constructor
     * @param {Hash} starterHash - hash from prior stream (if any) to be updated
     * with new stream here
     * @param {function} done Callback - This callback is called when
     * the hash computation is finished, this function need to be synchronous
     * and being call before the end of the stream object
     */
    constructor(starterHash, done) {
        super({});
        this.hash = starterHash || crypto.createHash('md5');
        this.done = done;
    }

    /**
     * This function will update the current md5 hash with the next chunk
     *
     * @param {Buffer|string} chunk - Chunk to compute
     * @param {string} encoding - Data encoding
     * @param {function} callback - Callback(err, chunk, encoding)
     * @return {undefined}
     */
    _transform(chunk, encoding, callback) {
        this.hash.update(chunk, encoding);
        this.done(this.hash);
        callback(null, chunk, encoding);
    }

}

module.exports = RelayMD5Sum;
