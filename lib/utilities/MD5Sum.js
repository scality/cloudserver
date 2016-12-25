const Transform = require('stream').Transform;
const crypto = require('crypto');

/**
 * This class is design to compute md5 hash at the same time as sending
 * data through a stream
 */
class MD5Sum extends Transform {

    /**
     * @constructor
     */
    constructor() {
        super({});
        this.hash = crypto.createHash('md5');
        this.completedHash = undefined;
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
        this.completedHash = this.hash.digest('hex');
        this.emit('hashed');
        callback(null);
    }

}

module.exports = MD5Sum;
