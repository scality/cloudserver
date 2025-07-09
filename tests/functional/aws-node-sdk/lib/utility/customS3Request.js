const { S3Client } = require('@aws-sdk/client-s3');
const querystring = require('querystring');

const getConfig = require('../../test/support/config');

const config = getConfig('default', { signatureVersion: 'v4' });
const s3 = new S3Client(config);

async function customS3Request(CommandCtor, params, buildParams, callback) {
    const command = new CommandCtor(params);
    // v3 does not support direct header/query mutation, so use middleware
    s3.middlewareStack.add(
        next => async args => {
            if (buildParams.headers) {
                Object.assign(args.request.headers, buildParams.headers);
            }
            if (buildParams.query) {
                const qs = querystring.stringify(buildParams.query);
                // eslint-disable-next-line no-param-reassign
                args.request.path += (args.request.path.includes('?') ? '&' : '?') + qs;
            }
            return next(args);
        },
        { step: 'build' }
    );
    try {
        const data = await s3.send(command);
        callback(null, {
            statusCode: data.$metadata?.httpStatusCode,
            headers: data.$metadata?.httpHeaders,
            body: JSON.stringify(data),
        });
    } catch (err) {
        callback(err, {
            statusCode: err.$metadata?.httpStatusCode,
            headers: err.$metadata?.httpHeaders,
            body: err.message,
        });
    }
}

module.exports = customS3Request;
