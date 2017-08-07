const stream = require('stream');
const { errors, s3middleware } = require('arsenal');
const MD5Sum = s3middleware.MD5Sum;
const constants = require('../../../constants');
const utils = {};

const padStrings = {
    partNumber: '00000',
    subPart: '00',
    part: '%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%',
    summaryPart:
        '++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++',
};

const splitter = '|';

/**
 * padLeft - left-pad a string
 * @param {string} str - string to pad
 * @param {string} category - category of string being padded
 * @param {number} padLength - length of string after padding
 * @return {string} - padded string
 */
utils.padLeft = (str, category, padLength) =>
    `${padStrings[category]}${str}`.substr(-padLength);

/**
 * padRight - right-pad a string
 * @param {string} str - string to pad
 * @param {string} category - category of string being padded
 * @param {number} padLength - length of string after padding
 * @return {string} - padded string
 */
utils.padRight = (str, category, padLength) =>
    `${str}${padStrings[category]}`.substr(0, padLength);

utils.getBlockId = (partNumber, subPartIndex) => {
    const paddedPartNumber = utils.padLeft(partNumber, 'partNumber', 5);
    const paddedSubPart = utils.padLeft(subPartIndex, 'subPart', 2);
    const blockId = `partNumber${paddedPartNumber}${splitter}` +
        `subPart${paddedSubPart}${splitter}`;
    return utils.padRight(blockId, 'part', 64);
};

utils.getSubPartInfo = dataContentLength => {
    const numberFullSubParts =
      Math.floor(dataContentLength / constants.maxSubPartSize);
    const remainder =
      dataContentLength - constants.maxSubPartSize * numberFullSubParts;
    const lastPartSize = remainder === 0 ? constants.maxSubPartSize : remainder;
    return {
        lastPartIndex: numberFullSubParts,
        lastPartSize,
    };
};

utils.getBase64MD5 = hexMD5 => Buffer.from(hexMD5, 'hex').toString('base64');

utils.getHexMD5 = base64MD5 => Buffer.from(base64MD5, 'base64').toString('hex');

utils.putSinglePart = (AzureClient, request, params, log, cb) => {
    const { bucketName, partNumber, mpuTempUploadKey, size, contentMD5 }
        = params;
    const totalSubParts = 1;
    const blockId = utils.getBlockId(partNumber, 0);
    const passThrough = new stream.PassThrough();
    const options = {};
    const method = 'uploadPart';
    if (contentMD5) {
        options.useTransactionalMD5 = true;
        options.transactionalContentMD5 = utils.getBase64MD5(contentMD5);
    }
    log.debug('part is less than max subpart size, putting single part');
    request.pipe(passThrough);
    return AzureClient.createBlockFromStream(blockId, bucketName,
        mpuTempUploadKey, passThrough, size, options, (err, result) => {
            if (err) {
                log.info('error putting part to Azure',
                { error: err.message, stack: err.stack, method });
                return cb(errors.InternalError);
            }
            const eTag = utils.getHexMD5(result.headers['content-md5']);
            return cb(null, eTag, totalSubParts);
        });
};

utils.putSubParts = (AzureClient, request, params, log, cb) => {
    const { bucketName, partNumber, mpuTempUploadKey, size } = params;
    const { lastPartIndex, lastPartSize } = utils.getSubPartInfo(size);
    let lengthCounter = 0;
    let currentStream = new stream.PassThrough();
    let finishedStreaming = false;
    const method = 'uploadPart';
    log.trace('data length is greater than max subpart size;' +
        'putting multiple parts');
    const hashedStream = new MD5Sum();
    request.pipe(hashedStream);
    function putNextSubPart(subPartIndex) {
        const blockId =
        utils.getBlockId(partNumber, subPartIndex);
        // NOTE: We try our best to calculate the sizes of each subpart before
        // streaming, but actual sizes may vary based on actual chunk sizes
        // in stream. Azure doesn't seem to use the content-length for anything
        // but returning the same content-length for the part.
        const subPartSize = subPartIndex === lastPartIndex ?
           lastPartSize : constants.maxSubPartSize;
        hashedStream.resume();
        return AzureClient.createBlockFromStream(blockId, bucketName,
            // eslint-disable-next-line
            mpuTempUploadKey, currentStream, subPartSize, {}, err => {
                if (err) {
                    log.info('error putting part to Azure',
                    { error: err.message, stack: err.stack, method });
                    return cb(errors.InternalError);
                }
                // NOTE: once this currentStream has ended, hashedStream
                // stream should be paused, having gotten to the point
                // we would stopper the data
                if (!finishedStreaming) {
                    return putNextSubPart(subPartIndex + 1);
                }
                const numberSubParts = subPartIndex + 1;
                log.trace('finished putting all subparts');
                hashedStream.on('hashed', () => {
                    log.trace('hashed event emitted');
                    hashedStream.removeAllListeners('hashed');
                    return cb(null, hashedStream.completedHash, numberSubParts);
                });
                // in case the hashed event was already emitted before the
                // event handler:
                if (hashedStream.completedHash) {
                    hashedStream.removeAllListeners('hashed');
                    return cb(null, hashedStream.completedHash, numberSubParts);
                }
            });
    }
    // start piping data once it is registered
    hashedStream.on('end', () => {
        finishedStreaming = true;
        currentStream.end();
    });
    hashedStream.on('data', data => {
        if (lengthCounter + data.length > constants.maxSubPartSize) {
            // stopper the data flow
            hashedStream.pause();
            // signal end of previous data put
            currentStream.end();
            // reset lengthCounter
            lengthCounter = 0;
            currentStream = new stream.PassThrough();
        }
        currentStream.write(data);
        lengthCounter += data.length;
    });
    putNextSubPart(0);
};

module.exports = utils;
