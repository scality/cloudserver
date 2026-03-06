const { Transform } = require('stream');
const { errors } = require('arsenal');
const { maximumAllowedPartSize } = require('../../../constants');

/**
 * This class is designed to handle the chunks sent in a streaming
 * unsigned playload trailer request. In this iteration, we are not checking
 * the checksums, but we are removing them from the stream.
 * S3C-9732 will deal with checksum verification.
 */
class TrailingChecksumTransform extends Transform {
    /**
     * @constructor
     * @param {object} log - logger object
     */
    constructor(log) {
        super({});
        this.log = log;
        this.chunkSizeBuffer = Buffer.alloc(0);
        this.bytesToDiscard = 0; // when trailing \r\n are present, we discard them but they can be in different chunks
        this.bytesToRead = 0; // when a chunk is advertised, the size is put here and we forward all bytes
        this.streamClosed = false;
        this.readingTrailer = false;
        this.trailerBuffer = Buffer.alloc(0);
        this.trailerName = null;
        this.trailerValue = null;
    }

    /**
     * This function is executed when there is no more data to be read but before the stream is closed
     * We will verify that the trailing checksum structure was upheld
     *
     * @param {function} callback - Callback(err, data)
     * @return {function} executes callback with err if applicable
     */
    _flush(callback) {
        if (!this.streamClosed) {
            this.log.error('stream ended without closing chunked encoding');
            return callback(errors.InvalidArgument);
        }
        return callback();
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

            // after the 0-size chunk, read the trailer line (e.g. "x-amz-checksum-crc32:YABb/g==")
            if (this.readingTrailer) {
                const combined = Buffer.concat([this.trailerBuffer, chunk]);
                const lineBreakIndex = combined.indexOf('\r\n');
                if (lineBreakIndex === -1) {
                    if (combined.byteLength > 1024) {
                        this.log.error('trailer line too long');
                        return callback(errors.InvalidArgument);
                    }
                    this.trailerBuffer = combined;
                    return callback();
                }
                const fullTrailer = combined.subarray(0, lineBreakIndex);
                this.trailerBuffer = Buffer.alloc(0);
                let trailerLine = fullTrailer.toString();
                // strip optional trailing \n before \r\n
                if (trailerLine.endsWith('\n')) {
                    trailerLine = trailerLine.slice(0, -1);
                }
                const colonIndex = trailerLine.indexOf(':');
                if (colonIndex > 0) {
                    this.trailerName = trailerLine.slice(0, colonIndex);
                    this.trailerValue = trailerLine.slice(colonIndex + 1);
                    this.emit('trailer', this.trailerName, this.trailerValue);
                } else {
                    this.log.error('invalid trailer line format', { trailerLine });
                    // TODO: test AWS
                    return callback(errors.InvalidArgument);
                }
                this.readingTrailer = false;
                this.streamClosed = true;
                break;
            }

            // we are now looking for the chunk size field
            // no need to look further than 10 bytes since the field cannot be bigger: the max
            // chunk size is 5GB (see constants.maximumAllowedPartSize)
            const lineBreakIndex = chunk.subarray(0, 10).indexOf('\r');
            const bytesToKeep = lineBreakIndex === -1 ? chunk.byteLength : lineBreakIndex;
            if (this.chunkSizeBuffer.byteLength + bytesToKeep > 10) {
                this.log.error('chunk size field too big', {
                    chunkSizeBuffer: this.chunkSizeBuffer.subarray(0, 11).toString('hex'),
                    chunkSizeBufferLength: this.chunkSizeBuffer.length,
                    truncatedChunk: chunk.subarray(0, 10).toString('hex'),
                });
                // if bigger, the chunk would be over 5 GB
                // returning early to avoid a DoS by memory exhaustion
                return callback(errors.InvalidArgument);
            }
            if (lineBreakIndex === -1) {
                // no delimiter, we'll keep the chunk for later
                this.chunkSizeBuffer = Buffer.concat([this.chunkSizeBuffer, chunk]);
                return callback();
            }

            this.chunkSizeBuffer = Buffer.concat([this.chunkSizeBuffer, chunk.subarray(0, lineBreakIndex)]);
            chunk = chunk.subarray(lineBreakIndex);

            // chunk-size is sent in hex
            const chunkSizeStr = this.chunkSizeBuffer.toString();
            const dataSize = parseInt(chunkSizeStr, 16);
            // we check that the parsing is correct (parseInt returns a partial parse when it fails)
            if (isNaN(dataSize) || dataSize.toString(16) !== chunkSizeStr.toLowerCase()) {
                this.log.error('invalid chunk size', { chunkSizeBuffer: chunkSizeStr });
                return callback(errors.InvalidArgument);
            }
            this.chunkSizeBuffer = Buffer.alloc(0);
            if (dataSize === 0) {
                // last chunk, no more data to read; enter trailer-reading mode
                // bytesToDiscard = 2 below will consume the \r\n after "0"
                this.readingTrailer = true;
            }
            if (dataSize > maximumAllowedPartSize) {
                this.log.error('chunk size too big', { dataSize });
                return callback(errors.EntityTooLarge);
            }
            this.bytesToRead = dataSize;
            this.bytesToDiscard = 2;
        }

        return callback();
    }
}

module.exports = TrailingChecksumTransform;
