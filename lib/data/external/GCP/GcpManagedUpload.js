const async = require('async');
const assert = require('assert');
const stream = require('stream');
const { errors } = require('arsenal');
const { minimumAllowedPartSize, gcpMaximumAllowedPartCount } =
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
     * GcpManagedUpload - class to mimic the upload method in AWS-SDK
     * To-Do: implement retry on failure like S3's upload
     * @param {GcpService} service - client object
     * @param {object} params - upload params
     * @param {string} params.Bucket - bucket name
     * @param {string} params.MPU - mpu bucket name
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
        this.partSize = options.partSize || minimumAllowedPartSize;
        this.queueSize = options.queueSize || 4;
        this.validateBody();
        this.setPartSize();
        // multipart information
        this.parts = {};
        this.uploadedParts = 0;
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
            Math.ceil(this.totalBytes / gcpMaximumAllowedPartCount);
        if (newPartSize > this.partSize) {
            this.partSize = newPartSize;
        }
        this.totalParts = Math.ceil(this.totalBytes / this.partSize);
        if (this.body instanceof Buffer && this.totalParts > 1) {
            this.slicedParts = sliceFn(this.body, this.partSize);
        }
    }

    /**
     * cleanUp - function that is called if GcpManagedUpload fails at any point,
     * perform clean up of used resources.
     * @param {Error} err - Error object
     * @return {undefined}
     */
    cleanUp(err) {
        // is only called when an error happens
        if (this.failed || this.completed) {
            return undefined;
        }
        this.failed = true;
        if (this.taskQueue && !this.killed) {
            this.killed = true;
            this.taskQueue.kill();
        }
        if (this.uploadId) {
            // if MPU was successfuly created
            return this.abortMPU(mpuErr => {
                if (mpuErr) {
                    logHelper(logger, 'error',
                        'GcpManagedUpload: abortMPU failed in cleanup', mpuErr);
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
     * uploadPartFn - queue upload task
     * @param {object} uploadObj - upload task object
     * @param {stream} uploadObj.stream - upload part stream object
     * @param {number} uploadObj.partNumber - upload part number
     * @param {function} callback - callback function to call when queue task
     * is completed
     * @return {undefined}
     */
    uploadPartFn(uploadObj, callback) {
        const params = {
            Bucket: this.mpuBucket,
            Key: this.params.Key,
            UploadId: this.uploadId,
            PartNumber: uploadObj.partNumber,
            Body: uploadObj.stream,
        };
        if (uploadObj.partNumber < this.totalParts) {
            params.ContentLength = this.partSize;
        } else {
            params.ContentLength =
                this.totalBytes - ((this.totalParts - 1) * this.partSize);
        }
        this.service.uploadPart(params, (err, res) => {
            if (err) {
                return callback(err);
            }
            this.parts[uploadObj.partNumber] = res.ETag;
            ++this.uploadedParts;
            return callback();
        });
    }

    /**
     * initQueue - initialize async queue
     * @return {undefined}
     */
    initQueue() {
        if (!this.taskQueue) {
            this.taskQueue = async.queue(
                this.uploadPartFn.bind(this), this.queueSize);
            this.taskQueue.drain = () => {
                if (this.doneReading &&
                    this.uploadedParts === this.totalParts) {
                    this.completeUpload();
                }
            };
            this.taskQueue.saturated = () => {
                if (!this.body.isPaused()) {
                    this.body.pause();
                }
            };
            this.taskQueue.unsaturated = () => {
                if (this.body.isPaused()) {
                    this.body.resume();
                }
            };
            this.taskQueue.error = err => this.cleanUp(err);
            // pause queue processing if uploadId has not been created
            if (!this.uploadId) {
                this.taskQueue.pause();
            }
        }
    }

    /**
     * createStream - create new transform stream for data chunks
     * @return {undefined}
     */
    createStream() {
        ++this.partNumber;
        this.targetStream = new stream.Transform({
            highWaterMark: this.partSize,
        });
        this.targetStream._transform =
            function fn(data, encoding, callback) {
                callback(null, data);
            };
        this.targetStream.on('error', err => this.cleanUp(err));
    }

    /**
     * handleData - body stream on data event handler
     * @param {string|buffer} data - stream data
     * @return {undefined}
     */
    handleData(data) {
        if (!this.targetStream) {
            this.createStream();
        }
        if (!this.currLength) {
            this.currLength = 0;
        }
        while ((this.currLength + data.length) >= this.partSize) {
            const readLength = this.partSize - this.currLength;
            this.totalChunkedBytes += readLength;
            this.targetStream.end(data.slice(0, readLength));
            this.taskQueue.push({
                stream: this.targetStream,
                partNumber: this.partNumber,
            });
            this.createStream();
            this.currLength = 0;
            // eslint-disable-next-line no-param-reassign
            data = data.slice(readLength);
        }
        if (data.length) {
            this.currLength += data.length;
            this.totalChunkedBytes += data.length;
            if (this.totalChunkedBytes >= this.totalBytes) {
                this.targetStream.end(data);
                this.taskQueue.push({
                    stream: this.targetStream,
                    partNumber: this.partNumber,
                });
            } else {
                this.targetStream.write(data);
            }
        }
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
            if (!this.taskQueue) {
                this.initQueue();
            }
            if (!this.uploadId) {
                const params = Object.assign({}, this.params);
                params.Bucket = this.mpuBucket;
                this.service.createMultipartUpload(params, (err, res) => {
                    if (err) {
                        return this.cleanUp(err);
                    }
                    this.uploadId = res.UploadId;
                    if (this.taskQueue.paused) {
                        this.taskQueue.resume();
                    }
                    return undefined;
                });
            }
            this.body.on('error', err => this.cleanUp(err))
            .on('data', data => this.handleData(data))
            .on('end', () => {
                logger.info('data chunking completed');
                this.doneReading = true;
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
}

module.exports = GcpManagedUpload;
