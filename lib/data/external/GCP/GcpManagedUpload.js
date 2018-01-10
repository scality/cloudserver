const async = require('async');
const assert = require('assert');
const stream = require('stream');
const { errors } = require('arsenal');
const { minimumAllowedPartSize, maximumAllowedPartCount } =
    require('../../../../constants');
const { createMpuList, logger } = require('./GcpUtils');
const { logHelper } = require('../utils');


function sliceFn(body, size) {
    const array = [];
    let partNumber = 1;
    for (let ind = 0; ind < body.length; ind += size) {
        array.push({
            Body: body.slice(ind, ind + size),
            PartNumber: partNumber++,
        });
    }
    return array;
}

class GcpManagedUpload {
    /**
     * GcpMangedUpload - class to mimic the upload method in AWS-SDK
     * To-Do: implement retry on failure like S3's upload
     * @param {GcpService} service - client object
     * @param {object} params - upload params
     * @param {string} params.Bucket - bucket name
     * @param {string} params.MPU - mpu bucket name
     * @param {string} params.Overflow - overflow bucket name
     * @param {string} params.Key - object key
     * @param {object} options - config setting for GcpManagedUpload object
     * @param {number} options.partSize - set object chunk size
     * @param {number} options.queueSize - set the number of concurrent upload
     * @return {object} - return an GcpManagedUpload object
     */
    constructor(service, params, options = {}) {
        this.service = service;
        this.params = params;
        this.mainBucket =
            this.params.Bucket || this.service.config.mainBucket;
        this.mpuBucket =
            this.params.MPU || this.service.config.mpuBucket;
        this.overflowBucket =
            this.params.Overflow || this.service.config.overflowBucket;

        this.partSize = minimumAllowedPartSize;
        this.queueSize = options.queueSize || 4;
        this.validateBody();
        this.setPartSize();
        // multipart information
        this.parts = {};
        this.uploadedParts = 0;
        this.activeParts = 0;
        this.partBuffers = [];
        this.partQueue = [];
        this.partBufferLength = 0;
        this.totalChunkedBytes = 0;
        this.partNumber = 0;
    }

    /**
     * validateBody - validate that body contains data to upload. If body is not
     * of type stream, it must then be of either string or buffer. If string,
     * convert to a Buffer type and split into chunks if body is large enough
     * @return {undefined}
     */
    validateBody() {
        this.body = this.params.Body;
        assert(this.body, errors.MissingRequestBodyError.customizeDescription(
            'Missing request body'));
        this.totalBytes = this.params.ContentLength;
        if (this.body instanceof stream) {
            assert.strictEqual(typeof this.totalBytes, 'number',
                errors.MissingContentLength.customizeDescription(
                'If body is a stream, ContentLength must be provided'));
        } else {
            if (typeof this.body === 'string') {
                this.body = Buffer.from(this.body);
            }
            this.totalBytes = this.body.byteLength;
            assert(this.totalBytes, errors.InternalError.customizeDescription(
                'Unable to perform upload'));
        }
    }

    setPartSize() {
        const newPartSize =
            Math.ceil(this.totalBytes / maximumAllowedPartCount);
        if (newPartSize > this.partSize) this.partSize = newPartSize;
        this.totalParts = Math.ceil(this.totalBytes / this.partSize);
        if (this.body instanceof Buffer && this.totalParts > 1) {
            this.slicedParts = sliceFn(this.body, this.partSize);
        }
    }

    /**
     * cleanUp - function that is called if GcpManagedUpload fails at any point,
     * perform clean up of used resources. Ends the request by calling an
     * internal callback function
     * @param {Error} err - Error object
     * @return {undefined}
     */
    cleanUp(err) {
        // is only called when an error happens
        if (this.failed || this.completed) {
            return undefined;
        }
        this.failed = true;
        if (this.uploadId) {
            // if MPU was successfuly created
            return this.abortMPU(mpuErr => {
                if (mpuErr) {
                    logHelper(logger, 'error',
                        'GcpMangedUpload: abortMPU failed in cleanup');
                }
                return this.callback(err);
            });
        }
        return this.callback(err);
    }

    /**
     * abortMPU - function that is called to remove a multipart upload
     * @param {function} callback - callback function to call to complete the
     * upload
     * @return {undefined}
     */
    abortMPU(callback) {
        const params = {
            Bucket: this.mainBucket,
            MPU: this.mpuBucket,
            Overflow: this.overflowBucket,
            UploadId: this.uploadId,
            Key: this.params.Key,
        };
        this.service.abortMultipartUpload(params, callback);
    }

    /**
     * completeUpload - function that is called to to complete a multipart
     * upload
     * @param {function} callback - callback function to call to complete the
     * upload
     * @return {undefined}
     */
    completeUpload() {
        if (this.failed || this.completed) {
            return undefined;
        }
        const params = {
            Bucket: this.mainBucket,
            MPU: this.mpuBucket,
            Overflow: this.overflowBucket,
            Key: this.params.Key,
            UploadId: this.uploadId,
            MultipartUpload: {},
        };
        params.MultipartUpload.Parts =
            createMpuList(params, 'parts', this.uploadedParts)
                .map(item =>
                    Object.assign(item, { ETag: this.parts[item.PartNumber] }));
        return this.service.completeMultipartUpload(params,
        (err, res) => {
            if (err) {
                return this.cleanUp(err);
            }
            this.completed = true;
            return this.callback(null, res);
        });
    }

    /**
     * send - function that is called to execute the method request
     * @param {function} callback - callback function to be called and stored
     * at the completion of the method
     * @return {undefined}
     */
    send(callback) {
        if (this.called || this.callback) {
            return undefined;
        }
        this.failed = false;
        this.called = true;
        this.callback = callback;
        if (this.totalBytes <= this.partSize) {
            return this.uploadSingle();
        }
        if (this.slicedParts) {
            return this.uploadBufferSlices();
        }
        if (this.body instanceof stream) {
            // stream type
            this.body.on('error', err => this.cleanUp(err))
            .on('readable', () => this.chunkStream())
            .on('end', () => {
                this.isDoneChunking = true;
                this.chunkStream();

                if (this.isDoneChunking && this.uploadedParts >= 1 &&
                    this.uploadedParts === this.totalParts) {
                    this.completeUpload();
                }
            });
        }
        return undefined;
    }

    /**
     * uploadSingle - perform a regular put object upload if the object is
     * small enough
     * @return {undefined}
     */
    uploadSingle() {
        if (this.failed || this.completed) {
            return undefined;
        }
        // use putObject to upload the single part object
        const params = Object.assign({}, this.params);
        params.Bucket = this.mainBucket;
        delete params.MPU;
        delete params.Overflow;
        return this.service.putObject(params, (err, res) => {
            if (err) {
                return this.cleanUp(err);
            }
            // return results from a putObject request
            this.completed = true;
            return this.callback(null, res);
        });
    }

    /**
     * uploadBufferSlices - perform a multipart upload for body of type string
     * or Buffer.
     * @return {undefined}
     */
    uploadBufferSlices() {
        if (this.failed || this.completed) {
            return undefined;
        }
        if (this.slicedParts.length <= 1 && this.totalParts) {
            // there is only one part
            return this.uploadSingle();
        }
        // multiple slices
        return async.series([
            // createMultipartUpload
            next => {
                const params = this.params;
                params.Bucket = this.mpuBucket;
                this.service.createMultipartUpload(params, (err, res) => {
                    if (!err) {
                        this.uploadId = res.UploadId;
                    }
                    return next(err);
                });
            },
            next => async.eachLimit(this.slicedParts, this.queueSize,
            (uploadPart, done) => {
                const params = {
                    Bucket: this.mpuBucket,
                    Key: this.params.Key,
                    UploadId: this.uploadId,
                    Body: uploadPart.Body,
                    PartNumber: uploadPart.PartNumber,
                };
                this.service.uploadPart(params, (err, res) => {
                    if (!err) {
                        this.parts[uploadPart.PartNumber] = res.ETag;
                        this.uploadedParts++;
                    }
                    return done(err);
                });
            }, next),
        ], err => {
            if (err) {
                return this.cleanUp(new Error(
                    'GcpManagedUpload: unable to complete upload'));
            }
            return this.completeUpload();
        });
    }

    /**
     * chunkStream - read stream up until the max chunk size then call an
     * uploadPart method on that chunk. If more than chunk size has be read,
     * move the extra bytes into a queue for the next read.
     * @return {undefined}
     */
    chunkStream() {
        const buf = this.body.read(this.partSize - this.partBufferLength) ||
            this.body.read();

        if (buf) {
            this.partBuffers.push(buf);
            this.partBufferLength += buf.length;
            this.totalChunkedBytes += buf.length;
        }

        let pbuf;
        if (this.partBufferLength >= this.partSize) {
            pbuf = Buffer.concat(this.partBuffers);
            this.partBuffers = [];
            this.partBufferLength = 0;

            if (pbuf.length > this.partSize) {
                const rest = pbuf.slice(this.partSize);
                this.partBuffers.push(rest);
                this.partBufferLength += rest.length;
                pbuf = pbuf.slice(0, this.partSize);
            }
            this.processChunk(pbuf);
        }

        // when chunking the last part
        if (this.isDoneChunking && !this.completeChunking) {
            this.completeChunking = true;
            pbuf = Buffer.concat(this.partBuffers);
            this.partBuffers = [];
            this.partBufferLength = 0;
            if (pbuf.length > 0) {
                this.processChunk(pbuf);
            } else {
                if (this.uploadedParts === 0) {
                    // this is a 0-byte object
                    this.uploadSingle();
                }
            }
        }

        this.body.read(0);
    }

    /**
     * processChunk - create a multipart request if one does not exist;
     * otherwise, call uploadChunk to upload a chunk
     * @param {Buffer} chunk - bytes to be uploaded
     * @return {undefined}
     */
    processChunk(chunk) {
        const partNumber = ++this.partNumber;
        if (!this.uploadId) {
            // if multipart upload does not exist
            if (!this.multipartReq) {
                const params = this.params;
                params.Bucket = this.mpuBucket;
                this.multipartReq =
                    this.service.createMultipartUpload(params, (err, res) => {
                        if (err) {
                            return this.cleanUp();
                        }
                        this.uploadId = res.UploadId;
                        this.uploadChunk(chunk, partNumber);
                        if (this.partQueue.length > 0) {
                            this.partQueue.forEach(
                                part => this.uploadChunk(...part));
                        }
                        return undefined;
                    });
            } else {
                this.partQueue.push([chunk, partNumber]);
            }
        } else {
            // queues chunks for upload
            this.uploadChunk(chunk, partNumber);
            this.activeParts++;
            if (this.activeParts < this.queueSize) {
                this.chunkStream();
            }
        }
    }

    /**
     * uploadChunk - perform the partUpload
     * @param {Buffer} chunk - bytes to be uploaded
     * @param {number} partNumber - upload object part number
     * @return {undefined}
     */
    uploadChunk(chunk, partNumber) {
        if (this.failed || this.completed) {
            return undefined;
        }
        const params = {
            Bucket: this.mpuBucket,
            Key: this.params.Key,
            UploadId: this.uploadId,
            PartNumber: partNumber,
            Body: chunk,
            ContentLength: chunk.length,
        };
        return this.service.uploadPart(params, (err, res) => {
            if (err) {
                return this.cleanUp(err);
            }
            this.parts[partNumber] = res.ETag;
            this.uploadedParts++;
            this.activeParts--;
            if (this.totalParts === this.uploadedParts &&
                this.isDoneChunking) {
                return this.completeUpload();
            }
            return this.chunkStream();
        });
    }
}

module.exports = GcpManagedUpload;
