const async = require('async');
const stream = require('stream');
const { lstatSync } = require('fs').lstatSync;
const { errors } = require('arsenal');
const { minimumAllowedPartSize, maximumAllowedPartCount } =
    require('../../../../constants');
const { createMpuList } = require('./GcpUtils');

function sliceFn(size) {
    this.array = [];
    let partNumber = 1;
    for (let ind = 0; ind < this.length; ind += size) {
        this.array.push({
            Body: this.slice(ind, ind + size),
            PartNumber: partNumber++,
        });
    }
    return this.array;
}

class GcpManagedUpload {
    /**
     * GcpMangedUpload - class to mimic the upload method in AWS-SDK
     * To-Do: implement retry on failure like S3's upload
     * @param {object} service - client object
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
        this.mpuBucket = this.params.MPU || this.service.config.mpuBucket;
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
        if (!this.body) {
            this.error = errors.InvalidArgument
                .customizeDescription('Body parameter is required');
        } else {
            if (this.params.ContentLength) {
                this.totalBytes = this.params.ContentLength;
            } else {
                if (this.body instanceof stream && this.body.path &&
                    typeof this.body.path === 'string') {
                    this.totalBytes = lstatSync(this.body.path).size;
                } // else this.body is a buffer
                if (typeof this.body === 'string') {
                    this.body = Buffer.from(this.body);
                }
                this.totalBytes = this.body.byteLength;
            }
        }
    }

    setPartSize() {
        if (this.totalBytes) {
            const newPartSize =
                Math.ceil(this.totalBytes / maximumAllowedPartCount);
            if (newPartSize > this.partSize) this.partSize = newPartSize;
            this.totalParts = Math.ceil(this.totalBytes / this.partSize);
        }
        if (this.body instanceof Buffer && this.totalParts > 1) {
            this.slicedParts = sliceFn.call(this.body, this.partSize);
        }
    }

    /**
     * abort - abort upload operation
     * @return {undefined}
     */
    abort() {
        // user called abort
        this.cleanUp(new Error('User called abort'));
    }

    /**
     * cleanup - function that is called if GcpManagedUpload fails at any point,
     * perform clean up of used resources. Ends the request by calling an
     * internal callback function
     * @param {Error} err - Error object
     * @return {undefined}
     */
    cleanUp(err) {
        // is only called when an error happens
        if (this.failed) return;

        this.failed = true;
        // clean variables
        this.activeParts = 0;
        this.partBuffers = [];
        this.partBuffertLength = 0;
        this.partNumber = 0;

        if (this.uploadId) {
            // if MPU was successfuly created
            this.abortMPU(mpuErr => {
                if (mpuErr) { // double error
                    this.callback(errors.InternalError
                        .customizeDescription(
                            'Unable to abort MPU after upload failure'));
                } else {
                    this.callback(err);
                }
            });
        } else {
            this.callback(err);
        }
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
        this.service.abortMultipartUpload(params, err => callback(err));
    }

    /**
     * completeUpload - function that is called to to complete a multipart
     * upload
     * @param {function} callback - callback function to call to complete the
     * upload
     * @return {undefined}
     */
    completeUpload(callback) {
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
                .map(item => {
                    Object.assign(item, {
                        ETag: this.parts[item.PartNumber],
                    });
                    return item;
                });
        this.service.completeMultipartUpload(params,
        (err, res) => {
            if (callback && typeof callback === 'function') {
                callback(err, res);
            } else {
                if (err) {
                    this.cleanUp(err);
                } else {
                    this.callback(null, res);
                }
            }
        });
    }

    /**
     * send - function that is called to execute the method request
     * @param {function} callback - callback function to be called and stored
     * at the completion of the method
     * @return {undefined}
     */
    send(callback) {
        this.failed = false;
        if (this.callback) return;
        this.callback = callback || function newCallback(err) {
            if (err) throw err;
        };
        if (this.error) {
            this.callback(this.error);
        } else {
            if (this.totalBytes <= this.partSize) {
                this.uploadSingle();
            } else if (this.slicedParts) {
                this.uploadBufferSlices();
            } else if (this.body instanceof stream) {
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
        }
    }

    /**
     * uploadSingle - perform a regular put object upload if the object is
     * small enough
     * @return {undefined}
     */
    uploadSingle() {
        // use putObject to upload the single part object
        const params = Object.assign({}, this.params);
        params.Bucket = this.mainBucket;
        delete params.MPU;
        delete params.Overflow;
        this.service.putObject(params, (err, res) => {
            if (err) {
                this.cleanUp(err);
            } else {
                // return results from a putObject request
                this.callback(null, res);
            }
        });
    }

    /**
     * uploadBufferSlices - perform a multipart upload for body of type string
     * or Buffer.
     * @return {undefined}
     */
    uploadBufferSlices() {
        if (this.slicedParts.length <= 1 && this.totalParts) {
            // there is only one part
            this.uploadSingle();
        } else {
            // multiple slices
            async.waterfall([
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
                next => async.mapLimit(this.slicedParts, this.queueSize,
                (uploadPart, moveOn) => {
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
                        moveOn(err);
                    });
                }, err => next(err)),
                next => this.completeUpload(next),
            ], (err, results) => {
                if (err) {
                    this.cleanUp(err);
                } else {
                    this.callback(null, results);
                }
            });
        }
    }

    /**
     * chunkStream - read stream up until the max chunk size then call an
     * uploadPart method on that chunk. If more than chunk size has be read,
     * move the extra bytes into a queue for the next read.
     * @return {undefined}
     */
    chunkStream() {
        if (this.activeParts > this.queueSize) return;

        const buf = this.body.read(this.partSize - this.partBufferLength) ||
            this.body.read();

        if (buf) {
            this.partBuffers.push(buf);
            this.partBufferLength += buf.length;
            this.totalChunkedBytes += buf.length;
        }

        let pbuf;
        if (this.partBufferLength >= this.partSize) {
            pbuf = this.partBuffers.length === 1 ?
                this.partBuffers[0] : Buffer.concat(this.partBuffers);
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
        if (this.isDoneChunking && !this.completed) {
            this.completed = true;
            pbuf = this.partBuffers.length === 1 ?
                this.partBuffers[0] : Buffer.concat(this.partBuffers);
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
                            this.cleanUp();
                        } else {
                            this.uploadId = res.UploadId;
                            this.uploadChunk(chunk, partNumber);
                            if (this.partQueue.length > 0) {
                                this.partQueue.forEach(
                                    part => this.uploadChunk(...part));
                            }
                        }
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
        const params = {
            Bucket: this.mpuBucket,
            Key: this.params.Key,
            UploadId: this.uploadId,
            PartNumber: partNumber,
            Body: chunk,
            ContentLength: chunk.length,
        };
        this.service.uploadPart(params, (err, res) => {
            if (err) {
                this.cleanUp(err);
            } else {
                this.parts[partNumber] = res.ETag;
                this.uploadedParts++;
                this.activeParts--;
                if (this.totalParts === this.uploadedParts &&
                    this.isDoneChunking) {
                    this.completeUpload();
                } else {
                    this.chunkStream();
                }
            }
        });
    }
}

module.exports = GcpManagedUpload;
