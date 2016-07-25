const Transform = require('stream').Transform;
const crypto = require('crypto');

/**
 * This class is design to compute md5 hash at the same time as sending
 * data through a stream
 */
class MD5Sum extends Transform {

    /**
     * @constructor
     * @param {function} done - Callback(hash) - This callback is called when
     * the hash computation is finished, this function need to be synchronous
     * and being call before the end of the stream object
     */
    constructor(done) {
        super({});
        this.hash = crypto.createHash('md5');
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
        callback(null, chunk, encoding);
    }

    /**
     * This function will end the hash computation
     *
     * @param {function} callback(err)
     * @return {undefined}
     */
    _flush(callback) {
        this.done(this.hash.digest('hex'));
        callback(null);
    }

}

module.exports = MD5Sum;
