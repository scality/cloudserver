const { auth, errors } = require('arsenal');
const busboy = require('@fastify/busboy');
const writeContinue = require('../../../utilities/writeContinue');
const fs = require('fs');
const path = require('path');
const os = require('os');

// per doc: https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-HTTPPOSTForms.html#HTTPPOSTFormDeclaration
const MAX_FIELD_SIZE = 20 * 1024; // 20KB
// per doc: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
const MAX_KEY_SIZE = 1024;

async function authenticateRequest(request, requestContexts, log) {
    return new Promise(resolve => {
        // TODO RING-45960 remove ignore for POST object here
        auth.server.doAuth(request, log, (err, userInfo, authorizationResults, streamingV4Params) =>
            resolve({ userInfo, authorizationResults, streamingV4Params }), 's3', requestContexts);
    });
}

async function parseFormData(request, response, requestContexts, log) {
    /* eslint-disable no-param-reassign */
    const formDataParser = busboy({ headers: request.headers });
    writeContinue(request, response);

    return new Promise((resolve, reject) => {
        request.formData = {};
        let totalFieldSize = 0;
        let fileEventData = null;
        let tempFileStream;
        let tempFilePath;
        let authResponse;
        let fileWrittenPromiseResolve;
        let formParserFinishedPromiseResolve;

        const fileWrittenPromise = new Promise((res) => { fileWrittenPromiseResolve = res; });
        const formParserFinishedPromise = new Promise((res) => { formParserFinishedPromiseResolve = res; });

        formDataParser.on('field', (fieldname, val) => {
            totalFieldSize += Buffer.byteLength(val, 'utf8');
            if (totalFieldSize > MAX_FIELD_SIZE) {
                return reject(errors.MaxPostPreDataLengthExceeded);
            }
            const lowerFieldname = fieldname.toLowerCase();
            if (lowerFieldname === 'key') {
                if (val.length > MAX_KEY_SIZE) {
                    return reject(errors.KeyTooLongError);
                } else if (val.length === 0) {
                    return reject(errors.InvalidArgument
                        .customizeDescription('User key must have a length greater than 0.'));
                }
            }
            request.formData[lowerFieldname] = val;
            return undefined;
        });

        formDataParser.on('file', async (fieldname, file, filename, encoding, mimetype) => {
            if (fileEventData) {
                file.resume(); // Resume the stream to drain and discard the file
                if (tempFilePath) {
                    fs.unlink(tempFilePath, unlinkErr => {
                        if (unlinkErr) {
                            log.error('Failed to delete temp file', { error: unlinkErr });
                        }
                    });
                }
                return reject(errors.InvalidArgument
                    .customizeDescription('POST requires exactly one file upload per request.'));
            }

            fileEventData = { fieldname, file, filename, encoding, mimetype };
            if (!('key' in request.formData)) {
                return reject(errors.InvalidArgument
                    .customizeDescription('Bucket POST must contain a field named '
                        + "'key'.  If it is specified, please check the order of the fields."));
            }
            // Replace `${filename}` with the actual filename
            request.formData.key = request.formData.key.replace('${filename}', filename);
            try {
                // Authenticate request before streaming file
                // TODO RING-45960 auth to be properly implemented
                authResponse = await authenticateRequest(request, requestContexts, log);

                // Create a temporary file to stream the file data
                // This is to finalize validation on form data before storing the file
                tempFilePath = path.join(os.tmpdir(), filename);
                tempFileStream = fs.createWriteStream(tempFilePath);

                file.pipe(tempFileStream);

                tempFileStream.on('finish', () => {
                    request.fileEventData = { ...fileEventData, file: tempFilePath };
                    fileWrittenPromiseResolve();
                });

                tempFileStream.on('error', (err) => {
                    log.trace('Error streaming file to temporary location', { error: err.message });
                    reject(errors.InternalError);
                });

                // Wait for both file writing and form parsing to finish
                return Promise.all([fileWrittenPromise, formParserFinishedPromise])
                    .then(() => resolve(authResponse))
                    .catch(reject);
            } catch (err) {
                return reject(err);
            }
        });

        formDataParser.on('finish', () => {
            if (!fileEventData) {
                return reject(errors.InvalidArgument
                    .customizeDescription('POST requires exactly one file upload per request.'));
            }
            return formParserFinishedPromiseResolve();
        });

        formDataParser.on('error', (err) => {
            log.trace('Error processing form data:', { error: err.message });
            request.unpipe(formDataParser);
            // Following observed AWS behaviour
            reject(errors.MalformedPOSTRequest);
        });

        request.pipe(formDataParser);
        return undefined;
    });
}

function getFileStat(filePath, log) {
    return new Promise((resolve, reject) => {
        fs.stat(filePath, (err, stats) => {
            if (err) {
                log.trace('Error getting file size', { error: err.message });
                return reject(errors.InternalError);
            }
            return resolve(stats);
        });
    });
}

async function processPostForm(request, response, requestContexts, log, callback) {
    if (!request.headers || !request.headers['content-type'].includes('multipart/form-data')) {
        const contentTypeError = errors.PreconditionFailed
            .customizeDescription('Bucket POST must be of the enclosure-type multipart/form-data');
        return process.nextTick(callback, contentTypeError);
    }
    try {
        const { userInfo, authorizationResults, streamingV4Params } =
            await parseFormData(request, response, requestContexts, log);

        const fileStat = await getFileStat(request.fileEventData.file, log);
        request.parsedContentLength = fileStat.size;
        request.fileEventData.file = fs.createReadStream(request.fileEventData.file);
        if (request.formData['content-type']) {
            request.headers['content-type'] = request.formData['content-type'];
        } else {
            request.headers['content-type'] = 'binary/octet-stream';
        }

        const authNames = { accountName: userInfo.getAccountDisplayName() };
        if (userInfo.isRequesterAnIAMUser()) {
            authNames.userName = userInfo.getIAMdisplayName();
        }
        log.addDefaultFields(authNames);

        return callback(null, userInfo, authorizationResults, streamingV4Params);
    } catch (err) {
        return callback(err);
    }
}

module.exports = {
    authenticateRequest,
    parseFormData,
    processPostForm,
    getFileStat,
};
