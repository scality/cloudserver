const { Transform } = require('stream');

const async = require('async');
const { errors } = require('arsenal');

const vault = require('../vault');
const constructChunkStringToSign = require('./constructChunkStringToSign');
const { log } = require('console');

/**
 * This class is designed to handle the chunks sent in a streaming
 * v4 Auth request
 */
class TrailingChecksumTransform extends Transform {
    /**
     * @constructor
     * @param {object} streamWithTrailingChecksum - the input stream
     * @param {function} errCb - callback called if an error occurs
     */
    constructor(log, errCb) {
        super({});
        this.log = log;
        this.errCb = errCb;
        this.currentData = undefined;
        this.dataCursor = 0;
        this.bytesToRead = 0; // when a chunk is advertised, the size is put here and we forward all bytes
        this.streamClosed = false;
    }

    /**
     * This function will remove the trailing checksum from the stream
     *
     * @param {Buffer} chunk - chunk from request body
     * @param {string} encoding - Data encoding
     * @param {function} callback - Callback(err, justDataChunk, encoding)
     * @return {function} executes callback with err if applicable
     */
    _transform(chunk, encoding, callback) {

        // const chunkWithDelimiter = 'BEGIN-CHUNK\n' + chunk.toString() + '\nEND-CHUNK\n';
        // return callback(null, chunkWithDelimiter, encoding);

        if (this.streamClosed) {
            return callback(null, chunk.subarray(0, 0), encoding);
        }

        if (this.bytesToRead > 0) {
            if (chunk.length <= this.bytesToRead) {
                // chunk is smaller than the advertised size
                // forward the whole chunk
                this.bytesToRead -= chunk.byteLength;
                // const newChunk = chunk + '\nEND CHUNK, remains=' + this.bytesToRead + '\n\n';
                const newChunk = chunk;
                return callback(null, newChunk, encoding);
            }
            // chunk is bigger than the advertised size
            const chunkData = chunk.subarray(0, this.bytesToRead);
            this.bytesToRead -= chunkData.byteLength;
            // const newChunk = chunkData + '\nEND CHUNK, remains=' + this.bytesToRead + '\n\n';
            return callback(null, chunkData, encoding);
            // not handling if there is additional data after the chunk, it should work on my test data
        }

        const lineBreakIndex = chunk.indexOf('\r\n');
        if (lineBreakIndex === -1) {
            // not handling when the chunk is split inside the trailing stuff
            // this.currentData = chunk;
            // if (this.currentData === undefined) {
            //     this.currentData = chunk;
            // } else {
            //     this.currentData = Buffer.concat([this.currentData, chunk]);
            // }
            return callback(null, chunk.subarray(0, 0), encoding);
        }

        // chunk-size is sent in hex
        const dataSize = Number.parseInt(chunk.subarray(0, lineBreakIndex).toString(), 16);
        if (Number.isNaN(dataSize)) {
            this.log.trace('chunk body did not contain valid size');
            return callback();
        }

        if (dataSize === 0) {
            // last chunk, no data to read
            this.streamClosed = true;
            return callback(null, chunk.subarray(0, 0), encoding);
        }

        this.log.trace('chunk-size', { dataSize });
        this.bytesToRead = dataSize;
        // const chunkData = 'BEGIN-CHUNK, remains:'+dataSize+'\n'+chunk.subarray(lineBreakIndex + 2);
        const chunkData = chunk.subarray(lineBreakIndex + 2);
        this.bytesToRead -= chunk.subarray(lineBreakIndex + 2).byteLength;
        return callback(null, chunkData, encoding);
    }
}

module.exports = TrailingChecksumTransform;
