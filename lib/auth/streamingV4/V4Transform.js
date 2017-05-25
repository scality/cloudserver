const { Transform } = require('stream');

const async = require('async');
const { errors } = require('arsenal');

const vault = require('../vault');
const constructChunkStringToSign = require('./constructChunkStringToSign');

/**
 * This class is designed to handle the chunks sent in a streaming
 * v4 Auth request
 */
class V4Transform extends Transform {

    /**
     * @constructor
     * @param {object} streamingV4Params - info for chunk authentication
     * @param {string} streamingV4Params.accessKey - requester's accessKey
     * @param {string} streamingV4Params.signatureFromRequest - signature
     * sent with headers
     * @param {string} streamingV4Params.region - region sent with auth header
     * @param {string} streamingV4Params.scopeDate - date sent with auth header
     * @param {string} streamingV4Params.timestamp - date parsed from headers
     * in ISO 8601 format: YYYYMMDDTHHMMSSZ
     * @param {string} streamingV4Params.credentialScope - items from auth
     * header plus the string 'aws4_request' joined with '/':
     * timestamp/region/aws-service/aws4_request
     * @param {object} log - logger object
     * @param {function} cb - callback to api
     */
    constructor(streamingV4Params, log, cb) {
        const { accessKey, signatureFromRequest, region, scopeDate, timestamp,
            credentialScope } = streamingV4Params;
        super({});
        this.log = log;
        this.cb = cb;
        this.accessKey = accessKey;
        this.region = region;
        this.scopeDate = scopeDate;
        this.timestamp = timestamp;
        this.credentialScope = credentialScope;
        this.lastSignature = signatureFromRequest;
        this.currentSignature = undefined;
        this.haveMetadata = false;
        // keep this as -1 to start since a seekingDataSize of 0
        // means that chunk is just metadata (as is the case with the
        // last chunk)
        this.seekingDataSize = -1;
        this.currentData = undefined;
        this.dataCursor = 0;
        this.currentMetadata = [];
        this.lastPieceDone = false;
        this.lastChunk = false;
    }

    /**
     * This function will parse the metadata portion of the chunk
     * @param {Buffer} remainingChunk - chunk sent from _transform
     * @return {object} response - if error, will return 'err' key with
     * arsenal error value.
     * if incomplete metadata, will return 'completeMetadata' key with
     * value false
     * if complete metadata received, will return 'completeMetadata' key with
     * value true and the key 'unparsedChunk' with the remaining chunk without
     * the parsed metadata piece
     */
    _parseMetadata(remainingChunk) {
        let remainingPlusStoredMetadata = remainingChunk;
        // have metadata pieces so need to add to the front of
        // remainingChunk
        if (this.currentMetadata.length > 0) {
            this.currentMetadata.push(remainingChunk);
            remainingPlusStoredMetadata = Buffer.concat(this.currentMetadata);
            // zero out stored metadata
            this.currentMetadata.length = 0;
        }
        let lineBreakIndex = remainingPlusStoredMetadata.indexOf('\r\n');
        if (lineBreakIndex < 0) {
            this.currentMetadata.push(remainingPlusStoredMetadata);
            return { completeMetadata: false };
        }
        let fullMetadata = remainingPlusStoredMetadata.slice(0,
            lineBreakIndex);

        // handle extra line break on end of data chunk
        if (fullMetadata.length === 0) {
            const chunkWithoutLeadingLineBreak = remainingPlusStoredMetadata
                .slice(2);
            // find second line break
            lineBreakIndex = chunkWithoutLeadingLineBreak.indexOf('\r\n');
            if (lineBreakIndex < 0) {
                this.currentMetadata.push(chunkWithoutLeadingLineBreak);
                return { completeMetadata: false };
            }
            fullMetadata = chunkWithoutLeadingLineBreak.slice(0,
                lineBreakIndex);
        }

        const splitMeta = fullMetadata.toString().split(';');
        this.log.trace('parsed full metadata for chunk', { splitMeta });
        if (splitMeta.length !== 2) {
            this.log.trace('chunk body did not contain correct ' +
            'metadata format');
            return { err: errors.InvalidArgument };
        }
        let dataSize = splitMeta[0];
        // chunk-size is sent in hex
        dataSize = Number.parseInt(dataSize, 16);
        if (Number.isNaN(dataSize)) {
            this.log.trace('chunk body did not contain valid size');
            return { err: errors.InvalidArgument };
        }
        let chunkSig = splitMeta[1];
        if (!chunkSig || chunkSig.indexOf('chunk-signature=') < 0) {
            this.log.trace('chunk body did not contain correct sig format');
            return { err: errors.InvalidArgument };
        }
        chunkSig = chunkSig.replace('chunk-signature=', '');
        this.currentSignature = chunkSig;
        this.haveMetadata = true;
        if (dataSize === 0) {
            this.lastChunk = true;
            return {
                completeMetadata: true,
            };
        }
        // + 2 to get \r\n at end
        this.seekingDataSize = dataSize + 2;
        this.currentData = Buffer.alloc(dataSize);

        return {
            completeMetadata: true,
            // start slice at lineBreak plus 2 to remove line break at end of
            // metadata piece since length of '\r\n' is 2
            unparsedChunk: remainingPlusStoredMetadata
                .slice(lineBreakIndex + 2),
        };
    }

    /**
     * Build the stringToSign and authenticate the chunk
     * @param {Buffer} dataToSend - chunk sent from _transform or null
     * if last chunk without data
     * @param {function} done - callback to _transform
     * @return {function} executes callback with err if applicable
     */
    _authenticate(dataToSend, done) {
        // use prior sig to construct new string to sign
        const stringToSign = constructChunkStringToSign(this.timestamp,
            this.credentialScope, this.lastSignature, dataToSend);
        this.log.trace('constructed chunk string to sign',
            { stringToSign });
        // once used prior sig to construct string to sign, reassign
        // lastSignature to current signature
        this.lastSignature = this.currentSignature;
        const vaultParams = {
            log: this.log,
            data: {
                accessKey: this.accessKey,
                signatureFromRequest: this.currentSignature,
                region: this.region,
                scopeDate: this.scopeDate,
                stringToSign,
                timestamp: this.timestamp,
                credentialScope: this.credentialScope,
            },
        };
        return vault.authenticateV4Request(vaultParams, null, err => {
            if (err) {
                this.log.trace('err from vault on streaming v4 auth',
                    { error: err, paramsSentToVault: vaultParams.data });
                return done(err);
            }
            return done();
        });
    }


    /**
     * This function will parse the chunk into metadata and data,
     * use the metadata to authenticate with vault and send the
     * data on to be stored if authentication passes
     *
     * @param {Buffer} chunk - chunk from request body
     * @param {string} encoding - Data encoding
     * @param {function} callback - Callback(err, justDataChunk, encoding)
     * @return {function }executes callback with err if applicable
     */
    _transform(chunk, encoding, callback) {
        // 'chunk' here is the node streaming chunk
        // transfer-encoding chunks should be of the format:
        // string(IntHexBase(chunk-size)) + ";chunk-signature=" +
        // signature + \r\n + chunk-data + \r\n
        // Last transfer-encoding chunk will have size 0 and no chunk-data.

        if (this.lastPieceDone) {
            const slice = chunk.slice(0, 10);
            this.log.trace('received chunk after end.' +
            'See first 10 bytes of chunk',
            { chunk: slice.toString() });
            return callback();
        }
        let unparsedChunk = chunk;
        let chunkLeftToEvaluate = true;
        return async.whilst(
            // test function
            () => chunkLeftToEvaluate,
            // async function
            done => {
                if (!this.haveMetadata) {
                    this.log.trace('do not have metadata so calling ' +
                    '_parseMetadata');
                    // need to parse our metadata
                    const parsedMetadataResults =
                        this._parseMetadata(unparsedChunk);
                    if (parsedMetadataResults.err) {
                        return done(parsedMetadataResults.err);
                    }
                    // if do not have full metadata get next chunk
                    if (!parsedMetadataResults.completeMetadata) {
                        chunkLeftToEvaluate = false;
                        return done();
                    }
                    // have metadata so reset unparsedChunk to remaining
                    // without metadata piece
                    unparsedChunk = parsedMetadataResults.unparsedChunk;
                }
                if (this.lastChunk) {
                    this.log.trace('authenticating final chunk with no data');
                    return this._authenticate(null, err => {
                        if (err) {
                            return done(err);
                        }
                        chunkLeftToEvaluate = false;
                        this.lastPieceDone = true;
                        return done();
                    });
                }
                if (unparsedChunk.length < this.seekingDataSize) {
                    // add chunk to currentData and get next chunk
                    unparsedChunk.copy(this.currentData, this.dataCursor);
                    this.dataCursor += unparsedChunk.length;
                    this.seekingDataSize -= unparsedChunk.length;
                    chunkLeftToEvaluate = false;
                    return done();
                }
                // parse just the next data piece without \r\n at the end
                // (therefore, minus 2)
                const nextDataPiece =
                    unparsedChunk.slice(0, this.seekingDataSize - 2);
                // add parsed data piece to other currentData pieces
                // so that this.currentData is the full data piece
                nextDataPiece.copy(this.currentData, this.dataCursor);
                return this._authenticate(this.currentData, err => {
                    if (err) {
                        return done(err);
                    }
                    unparsedChunk =
                        unparsedChunk.slice(this.seekingDataSize);
                    this.push(this.currentData);
                    this.haveMetadata = false;
                    this.seekingDataSize = -1;
                    this.currentData = undefined;
                    this.dataCursor = 0;
                    chunkLeftToEvaluate = unparsedChunk.length > 0;
                    return done();
                });
            },
            // final callback
            err => {
                if (err) {
                    return this.cb(err);
                }
                // get next chunk
                return callback();
            }
        );
    }
}

module.exports = V4Transform;
