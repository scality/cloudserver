import { S3 } from 'aws-sdk';
import querystring from 'querystring';

import getConfig from '../../test/support/config';

const config = getConfig('default', { signatureVersion: 'v4' });
const s3 = new S3(config);

export default function customS3Request(action, params, buildParams, callback) {
    const method = action.bind(s3);
    const request = method(params);
    const { headers, query } = buildParams;
    // modify underlying http request object created by aws sdk
    request.on('build', () => {
        Object.assign(request.httpRequest.headers, headers);
        if (query) {
            const qs = querystring.stringify(query);
            // NOTE: that this relies on there not being a query string in the
            // first place; if there is a qs then we have to search for ? and
            // append &qs at the end of the string, if ? is not followed by ''
            request.httpRequest.path = `${request.httpRequest.path}?${qs}`;
        }
    });
    request.on('success', response => {
        const resData = {
            statusCode: response.httpResponse.statusCode,
            headers: response.httpResponse.headers,
            body: response.httpResponse.body.toString('utf8'),
        };
        callback(null, resData);
    });
    request.on('error', err => {
        const resData = {
            statusCode: request.response.httpResponse.statusCode,
            headers: request.response.httpResponse.headers,
            body: request.response.httpResponse.body.toString('utf8'),
        };
        callback(err, resData);
    });
    request.send();
}
