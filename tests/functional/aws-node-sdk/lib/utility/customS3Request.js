const { S3Client } = require('@aws-sdk/client-s3');
const { HttpRequest } = require('@smithy/protocol-http');
const querystring = require('querystring');

const getConfig = require('../../test/support/config');

const config = getConfig('default');

// Custom middleware to modify requests in AWS SDK v3 without mutating args
const customRequestMiddleware = buildParams => next => async args => {
    const { headers, query } = buildParams;

    const prevReq = args.request;
    const base = prevReq instanceof HttpRequest ? prevReq : new HttpRequest(prevReq);

    let newHeaders = base.headers || {};
    if (headers) {
        newHeaders = { ...newHeaders, ...headers };
    }

    let newQuery = base.query || {};
    if (query) {
        const extra = querystring.parse(querystring.stringify(query));
        newQuery = { ...newQuery, ...extra };
    }

    const newReq = new HttpRequest({
        ...base,
        headers: newHeaders,
        query: newQuery,
    });

    return next({ ...args, request: newReq });
};

// In AWS SDK v3, we need to handle requests differently
// This is a simplified version that works with basic S3 operations
async function customS3Request(CommandClass, params, buildParams) {
        const customS3 = new S3Client({ ...config });

        customS3.middlewareStack.add(
            customRequestMiddleware(buildParams),
            { step: 'build', name: 'customRequestMiddleware', tags: ['CUSTOM'] }
        );

        const command = new CommandClass(params);
        const response = await customS3.send(command);

        const resData = {
            statusCode: 200,
            headers: response.$metadata?.httpHeaders || {},
            body: JSON.stringify(response),
        };

        return resData;
}

module.exports = customS3Request;
