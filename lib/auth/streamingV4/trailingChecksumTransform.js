const { Transform } = require('stream');
const { errors } = require('arsenal');

/**
 * This class is designed to handle the chunks sent in a streaming
 * v4 Auth request
 */
class TrailingChecksumTransform extends Transform {
    /**
     * @constructor
     * @param {object} log - logger object
     * @param {function} errCb - callback called if an error occurs
     */
    constructor(log, errCb) {
        super({});
        this.log = log;
        this.errCb = errCb;
        this.chunkSizeBuffer = Buffer.alloc(0);
        this.bytesToDiscard = 0; // when trailing \r\n are present, we discard them but they can be in different chunks
        this.bytesToRead = 0; // when a chunk is advertised, the size is put here and we forward all bytes
        this.streamClosed = false;
    }

    /**
     * This function will remove the trailing checksum from the stream
     *
     * @param {Buffer} chunkInput - chunk from request body
     * @param {string} encoding - Data encoding
     * @param {function} callback - Callback(err, justDataChunk, encoding)
     * @return {function} executes callback with err if applicable
     */
    _transform(chunkInput, encoding, callback) {
        let chunk = chunkInput;
        while (chunk.byteLength > 0 && !this.streamClosed) {
            if (this.bytesToDiscard > 0) {
                const toDiscard = Math.min(this.bytesToDiscard, chunk.byteLength);
                chunk = chunk.subarray(toDiscard);
                this.bytesToDiscard -= toDiscard;
                continue;
            }
            // forward up to bytesToRead bytes from the chunk, restart processing on leftover
            if (this.bytesToRead > 0) {
                const toRead = Math.min(this.bytesToRead, chunk.byteLength);
                this.push(chunk.subarray(0, toRead));
                chunk = chunk.subarray(toRead);
                this.bytesToRead -= toRead;
                if (this.bytesToRead === 0) {
                    this.bytesToDiscard = 2;
                }
                continue;
            }

            const lineBreakIndex = chunk.indexOf('\r');
            if (lineBreakIndex === -1) {
                if (this.chunkSizeBuffer.byteLength + chunk.byteLength > 10) {
                    this.log.info('chunk size field too big', {
                        chunkSizeBuffer: this.chunkSizeBuffer.toString(),
                        truncatedChunk: chunk.subarray(0, 16).toString(),
                    });
                    // if bigger, the chunk would be over 5 GB
                    // returning early to avoid a DoS by memory exhaustion
                    return callback(errors.InvalidArgument);
                }
                // no delimiter, we'll keep the chunk for later
                this.chunkSizeBuffer = Buffer.concat([this.chunkSizeBuffer, chunk]);
                return callback();
            }

            this.chunkSizeBuffer = Buffer.concat([this.chunkSizeBuffer, chunk.subarray(0, lineBreakIndex)]);
            chunk = chunk.subarray(lineBreakIndex);

            // chunk-size is sent in hex
            if (!/^[0-9a-fA-F]+$/.test(this.chunkSizeBuffer.toString())) {
                this.log.info('chunk size is not a valid hex number', {
                    chunkSizeBuffer: this.chunkSizeBuffer.toString(),
                });
                return callback(errors.InvalidArgument);
            }
            const dataSize = Number.parseInt(this.chunkSizeBuffer.toString(), 16);
            if (Number.isNaN(dataSize)) {
                this.log.error('unable to parse chunk size', {
                    chunkSizeBuffer: this.chunkSizeBuffer.toString(),
                });
                return callback(errors.InvalidArgument);
            }
            this.chunkSizeBuffer = Buffer.alloc(0);
            if (dataSize === 0) {
                // TODO: check if the checksum is correct
                // last chunk, no more data to read, the stream is closed
                this.streamClosed = true;
            }
            this.bytesToRead = dataSize;
            this.bytesToDiscard = 2;
        }

        return callback();
    }
}

module.exports = TrailingChecksumTransform;
