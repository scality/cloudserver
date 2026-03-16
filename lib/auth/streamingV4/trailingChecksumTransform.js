const { Transform } = require('stream');
const { errors, errorInstances } = require('arsenal');
const { maximumAllowedPartSize } = require('../../../constants');

const incompleteBodyError = errorInstances.IncompleteBody.customizeDescription(
    'The request body terminated unexpectedly');

/**
 * This class handles the chunked-upload body format used by
 * STREAMING-UNSIGNED-PAYLOAD-TRAILER requests. It strips the chunk-size
 * headers and trailing checksum trailer from the stream, forwarding only
 * the raw object data. The trailer name and value are emitted via a
 * 'trailer' event so that ChecksumTransform can validate the checksum.
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
        if (!this.streamClosed && this.readingTrailer && this.trailerBuffer.length === 0) {
            // Nothing came after "0\r\n", don't fail.
            // If the x-amz-trailer header was present then the trailer is required and ChecksumTransform will fail.
            return callback();
        } else if (!this.streamClosed && this.readingTrailer && this.trailerBuffer.length !== 0) {
            this.log.error('stream ended without trailer "\r\n"');
            return callback(incompleteBodyError);
        } else if (!this.streamClosed && !this.readingTrailer) {
            this.log.error('stream ended without closing chunked encoding');
            return callback(incompleteBodyError);
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
                        return callback(errors.MalformedTrailerError);
                    }
                    // The trailer is not complete yet, continue.
                    this.trailerBuffer = combined;
                    return callback();
                }
                this.trailerBuffer = Buffer.alloc(0);
                const fullTrailer = combined.subarray(0, lineBreakIndex);
                if (fullTrailer.length === 0) {
                    // The trailer is empty, stop reading.
                    this.readingTrailer = false;
                    this.streamClosed = true;
                    return callback();
                }
                let trailerLine = fullTrailer.toString();
                // Some clients terminate the trailer with \n\r\n instead of
                // just \r\n, producing a trailing \n in the parsed line.
                if (trailerLine.endsWith('\n')) {
                    trailerLine = trailerLine.slice(0, -1);
                }
                const colonIndex = trailerLine.indexOf(':');
                if (colonIndex > 0) {
                    this.trailerName = trailerLine.slice(0, colonIndex).trim();
                    this.trailerValue = trailerLine.slice(colonIndex + 1).trim();
                    this.emit('trailer', this.trailerName, this.trailerValue);
                } else {
                    this.log.error('incomplete trailer missing ":"', { trailerLine });
                    return callback(incompleteBodyError);
                }
                this.readingTrailer = false;
                this.streamClosed = true;
                // The trailer \r\n is the last bytes of the stream per the AWS
                // chunked upload format, so any remaining bytes are discarded.
                return callback();
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
