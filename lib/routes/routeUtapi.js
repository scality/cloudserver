const url = require('url');
const http = require('http');
const { errors } = require('arsenal');
const { responseJSONBody } = require('arsenal').s3routes.routesUtils;
const { config } = require('../Config');

function _createUtapiRequestOptions(req) {
    const parsedUrl = url.parse(req.url, true);
    // path will begin with /_/utapi
    const basePath = `${parsedUrl.pathname.substring(8)}`;
    const utapiPath = parsedUrl.search ? `${basePath}${parsedUrl.search}` : `${basePath}`;
    const utapiPort = config.utapi.port || 8100;
    const utapiHost = config.utapi.host || 'localhost';

    return {
        path: utapiPath,
        port: utapiPort,
        host: utapiHost,
        method: 'POST',
        headers: {
            'content-type': 'application/json',
            'cache-control': 'no-cache',
        },
        rejectUnauthorized: false,
    };
}

function _respond(response, payload, log) {
    const body = typeof payload === 'object' ?
        JSON.stringify(payload) : payload;
    const httpHeaders = {
        'x-amz-id-2': log.getSerializedUids(),
        'x-amz-request-id': log.getSerializedUids(),
        'content-type': 'application/json',
        'content-length': Buffer.byteLength(body),
    };
    response.writeHead(200, httpHeaders);
    response.end(body, 'utf8');
}


function routeUtapi(clientIP, request, response, log) {
    log.debug('route request', { method: 'routeUtapi' });
    let reqBody = '';
    request.on('data', chunk => {
        reqBody += chunk.toString();
    });
    request.on('end', () => {
        const options = _createUtapiRequestOptions(request);
        const utapiRequest = http.request(options, res => {
            const resBody = [];
            res.setEncoding('utf8');
            res.on('data', chunk => resBody.push(chunk));
            res.on('end', () => {
                const responseBody = JSON.parse(resBody.join(''));
                if (res.statusCode >= 200 && res.statusCode < 300) {
                    return _respond(response, responseBody, log);
                }
                log.error('Utapi request failed', {
                    statusCode: res.statusCode,
                    body: responseBody });
                return responseJSONBody(errors.InternalError, null,
                    response, log);
            });
        });
        utapiRequest.write(reqBody);
        utapiRequest.end();
    });
}

module.exports = routeUtapi;
