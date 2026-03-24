const { Transform } = require('stream');
const { errors, errorInstances } = require('arsenal');
const { maximumAllowedPartSize } = require('../../../constants');

const incompleteBodyError = errorInstances.IncompleteBody.customizeDescription(
    'The request body terminated unexpectedly');

const CR = '\r'.charCodeAt(0);
const LF = '\n'.charCodeAt(0);

const State = Object.freeze({
    READ_CHUNK_LEN: 0,
    READ_CHUNK_LEN_LF: 1,
    READ_CHUNK_DATA: 2,
    READ_CHUNK_DATA_CR: 3,
    READ_CHUNK_DATA_LF: 4,
    COMPLETED_NO_TRAILER: 5,
    READ_TRAILER_CHECKSUM: 6,
    READ_TRAILER_CHECKSUM_LF: 7,
    COMPLETED_WITH_TRAILER: 8,
});

class Fsm {
    constructor() {
        this.state = State.READ_CHUNK_LEN;
        this.buffer = Buffer.alloc(1024);
        this.bufferOffset = 0;
        this.chunkRemaining = 0;
    }

    /**
     * Advance the FSM by one input buffer.
     *
     * Parses the AWS chunked-upload framing byte-by-byte, forwarding raw object
     * data via `push` and signalling the trailer via `emit`.
     *
     * @param {Buffer} data - incoming bytes from the transport stream
     * @param {function} push - bound Transform.push; called with each slice of
     *   decoded object data
     * @param {function} emit - bound Transform.emit; called as
     *   emit('trailer', name, value) when the trailing checksum line is parsed
     * @param {object} log - request logger
     * @return {ArsenalError|null} an Arsenal error if the framing is invalid,
     *   or null on success
     */
    step(data, push, emit, log) {
        let idx = 0;

        while (idx < data.byteLength) {
            switch (this.state) {
                case State.READ_CHUNK_LEN: {
                    const byte = data[idx++];
                    if (byte === CR) {
                        this.state = State.READ_CHUNK_LEN_LF;
                        continue;
                    }

                    // Accumulate all bytes. AWS accepts whitespace before and after the hex digits.
                    // This is a safety-net against excessively long fields; the real digit-count
                    // limit (> 9 trimmed chars) is enforced in READ_CHUNK_LEN_LF after trim().
                    this.buffer[this.bufferOffset++] = byte;
                    if (this.bufferOffset >= this.buffer.length) {
                        log.error('chunk length field too large');
                        return errors.InvalidArgument;
                    }

                    continue;
                }
                case State.READ_CHUNK_LEN_LF: {
                    const byte = data[idx++];
                    if (byte !== LF) {
                        log.error('expected LF after chunk length CR', { byte });
                        return errors.InvalidArgument;
                    }

                    const chunkLenStr = this.buffer.toString('ascii', 0, this.bufferOffset).trim();
                    if (chunkLenStr.length === 0) {
                        log.error('empty chunk length field');
                        return errors.InvalidArgument;
                    }
                    // AWS does not do this check, it returns 500 if it is too large.
                    if (chunkLenStr.length > 9) {
                        log.error('chunk length field too large', { chunkLenStr });
                        return errors.InvalidArgument;
                    }

                    // Check it is HEX.
                    if (!/^[0-9a-fA-F]+$/.test(chunkLenStr)) {
                        log.error('invalid chunk size', { chunkLenStr });
                        return errors.InvalidArgument;
                    }
                    const chunkLen = parseInt(chunkLenStr, 16);

                    if (chunkLen > maximumAllowedPartSize) {
                        log.error('chunk size too big', { chunkLen });
                        return errors.EntityTooLarge;
                    }

                    this.bufferOffset = 0;

                    if (chunkLen === 0) {
                        this.state = State.COMPLETED_NO_TRAILER;
                        continue;
                    }

                    this.chunkRemaining = chunkLen;
                    this.state = State.READ_CHUNK_DATA;

                    continue;
                }
                case State.READ_CHUNK_DATA: {
                    // subarray clamps to buffer bounds, so toPush may be shorter than chunkRemaining
                    // when the chunk spans multiple _transform calls.
                    const toPush = data.subarray(idx, idx + this.chunkRemaining);
                    push(toPush);
                    this.chunkRemaining -= toPush.byteLength;
                    idx += toPush.byteLength;
                    if (this.chunkRemaining === 0) {
                        this.state = State.READ_CHUNK_DATA_CR;
                    }
                    continue;
                }
                case State.READ_CHUNK_DATA_CR: {
                    const byte = data[idx++];
                    if (byte !== CR) {
                        log.error('expected CR after chunk data', { byte });
                        return errors.InvalidArgument;
                    }

                    this.state = State.READ_CHUNK_DATA_LF;

                    continue;
                }
                case State.READ_CHUNK_DATA_LF: {
                    const byte = data[idx++];
                    if (byte !== LF) {
                        log.error('expected LF after chunk data CR', { byte });
                        return errors.InvalidArgument;
                    }

                    // Reset buffer state before reading the next chunk header.
                    this.bufferOffset = 0;
                    this.chunkRemaining = 0;
                    this.state = State.READ_CHUNK_LEN;

                    continue;
                }
                case State.COMPLETED_NO_TRAILER:
                    // A byte arrived after "0\r\n" — transition to trailer reading.
                    // bufferOffset is reset here so the shared buffer is clean for the trailer.
                    this.state = State.READ_TRAILER_CHECKSUM;
                    this.bufferOffset = 0;
                    continue;
                case State.READ_TRAILER_CHECKSUM: {
                    const byte = data[idx++];
                    if (byte === CR) {
                        this.state = State.READ_TRAILER_CHECKSUM_LF;
                        continue;
                    }

                    // Accumulate all bytes, AWS accepts white spaces before and after the CRLF
                    this.buffer[this.bufferOffset++] = byte;
                    if (this.bufferOffset === this.buffer.length) {
                        log.error('trailer field too large');
                        return errors.MalformedTrailerError;
                    }

                    continue;
                }
                case State.READ_TRAILER_CHECKSUM_LF: {
                    const byte = data[idx++];
                    if (byte !== LF) {
                        log.error('expected LF after trailer CR', { byte });
                        return errors.InvalidArgument;
                    }

                    if (this.bufferOffset > 0) {
                        const trailerLine = this.buffer.toString('ascii', 0, this.bufferOffset).trim();
                        const colonIndex = trailerLine.indexOf(':');
                        if (colonIndex > 0) {
                            const trailerName = trailerLine.slice(0, colonIndex).trim();
                            const trailerValue = trailerLine.slice(colonIndex + 1).trim();
                            emit('trailer', trailerName, trailerValue);
                        } else {
                            log.error('incomplete trailer missing ":"', { trailerLine });
                            return incompleteBodyError;
                        }
                    }

                    this.state = State.COMPLETED_WITH_TRAILER;

                    continue;
                }
                case State.COMPLETED_WITH_TRAILER:
                    // We successfully parsed the trailing checksum, discard all extra data.
                    return null;
            }
        }

        return null;
    }
}

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
        this.log.addDefaultFields({ component: 'TrailingChecksumTransform' });
        this.fsm = new Fsm();
    }

    /**
     * This function is executed when there is no more data to be read but before the stream is closed
     * We will verify that the trailing checksum structure was upheld
     *
     * @param {function} callback - Callback(err, data)
     * @return {function} executes callback with err if applicable
     */
    _flush(callback) {
        // COMPLETED means we saw "0\r\n" but no trailer bytes after,
        // ChecksumTransform will enforce the trailer if x-amz-trailer was present.
        if (this.fsm.state !== State.COMPLETED_WITH_TRAILER && this.fsm.state !== State.COMPLETED_NO_TRAILER) {
            this.log.error('stream ended without closing chunked encoding',
                { state: this.fsm.state });
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
        const err = this.fsm.step(chunkInput, this.push.bind(this), this.emit.bind(this), this.log);
        if (err) {
            return callback(err);
        }
        return callback();
    }
}

module.exports = TrailingChecksumTransform;
