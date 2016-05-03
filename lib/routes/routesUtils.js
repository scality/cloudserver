import data from '../data/wrapper';

/**
 * setCommonResponseHeaders - Set HTTP response headers
 * @param {object} headers - key and value of new headers to add
 * @param {object} response - http response object
 * @param {object} log - Werelogs logger
 * @return {object} response - response object with additional headers
 */
function setCommonResponseHeaders(headers, response, log) {
    if (headers && typeof headers === 'object') {
        log.trace('setting response headers', { headers });
        Object.keys(headers).forEach(key => {
            if (headers[key]) {
                response.setHeader(key, headers[key]);
            }
        });
    }
    response.setHeader('server', 'AmazonS3');
    // to be expanded in further implementation of logging of requests
    response.setHeader('x-amz-id-2', log.getSerializedUids());
    response.setHeader('x-amz-request-id', log.getSerializedUids());
    return response;
}
/**
 * okHeaderResponse - Response with only headers, no body
 * @param {object} headers - key and value of new headers to add
 * @param {object} response - http response object
 * @param {number} httpCode -- http response code
 * @param {object} log - Werelogs logger
 * @return {object} response - response object with additional headers
 */
function okHeaderResponse(headers, response, httpCode, log) {
    log.trace('sending success header response');
    setCommonResponseHeaders(headers, response, log);
    log.debug('response http code', { httpCode });
    response.writeHead(httpCode);
    return response.end(() => {
        log.end().info('responded to request', {
            httpCode: response.statusCode,
        });
    });
}

/**
 * okXMLResponse - Response with XML body
 * @param {string} xml - XML body as string
 * @param {object} response - http response object
 * @param {object} log - Werelogs logger
 * @return {object} response - response object with additional headers
 */
function okXMLResponse(xml, response, log) {
    log.trace('sending success xml response');
    setCommonResponseHeaders(null, response, log);
    response.writeHead(200, { 'Content-type': 'application/xml' });
    log.debug('response http code', { httpCode: 200 });
    log.trace('xml response', { xml });
    return response.end(xml, 'utf8', () => {
        log.end().info('responded with XML', {
            httpCode: response.statusCode,
        });
    });
}

function errorXMLResponse(errCode, response, log) {
    log.trace('sending error xml response', { errCode });
    /*
    <?xml version="1.0" encoding="UTF-8"?>
     <Error>
     <Code>NoSuchKey</Code>
     <Message>The resource you requested does not exist</Message>
     <Resource>/mybucket/myfoto.jpg</Resource>
     <RequestId>4442587FB7D0A2F9</RequestId>
     </Error>
     */
    const xml = [];
    xml.push(
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<Error>',
        `<Code>${errCode.message}</Code>`,
        `<Message>${errCode.description}</Message>`,
        '<Resource></Resource>',
        `<RequestId>${log.getSerializedUids()}</RequestId>`,
        '</Error>'
    );
    setCommonResponseHeaders(null, response, log);
    response.writeHead(errCode.code, { 'Content-type': 'application/xml' });
    return response.end(xml.join(''), 'utf8', () => {
        log.end().info('responded with error XML', {
            httpCode: response.statusCode,
        });
    });
}

/**
 * Modify response headers for an objectGet or objectHead request
 * @param {object} overrideHeaders - headers in this object override common
 * headers. These are extracted from the request object
 * @param {object} resHeaders - object with common response headers
 * @param {object} response - router's response object
 * @param {object} log - Werelogs logger
 * @return {object} response - modified response object
 */
function okContentHeadersResponse(overrideHeaders, resHeaders, response, log) {
    const addHeaders = {};
    Object.assign(addHeaders, resHeaders);

    if (overrideHeaders['response-content-type']) {
        addHeaders['Content-Type'] = overrideHeaders['response-content-type'];
    }
    if (overrideHeaders['response-content-language']) {
        addHeaders['Content-Language'] =
            overrideHeaders['response-content-language'];
    }
    if (overrideHeaders['response-expires']) {
        addHeaders.Expires = overrideHeaders['response-expires'];
    }
    if (overrideHeaders['response-cache-control']) {
        addHeaders['Cache-Control'] = overrideHeaders['response-cache-control'];
    }
    if (overrideHeaders['response-content-disposition']) {
        addHeaders['Content-Disposition'] =
        overrideHeaders['response-content-disposition'];
    }
    if (overrideHeaders['response-content-encoding']) {
        addHeaders['Content-Encoding'] =
            overrideHeaders['response-content-encoding'];
    }

    setCommonResponseHeaders(addHeaders, response, log);
    log.debug('response http code', { httpCode: 200 });
    response.writeHead(200);
    return response;
}

const routesUtils = {
    /**
     * @param {string} errCode - S3 error Code
     * @param {string} xml - xml body as string conforming to S3's spec.
     * @param {object} response - router's response object
     * @param {object} log - Werelogs logger
     * @return {function} - error or success response utility
     */
    responseXMLBody(errCode, xml, response, log) {
        if (errCode && !response.headersSent) {
            return errorXMLResponse(errCode, response, log);
        }
        if (!response.headersSent) {
            return okXMLResponse(xml, response, log);
        }
    },

    /**
     * @param {string} errCode - S3 error Code
     * @param {string} resHeaders - headers to be set for the response
     * @param {object} response - router's response object
     * @param {number} httpCode - httpCode to set in response
     *   If none provided, defaults to 200.
     * @param {object} log - Werelogs logger
     * @return {function} - error or success response utility
     */
    responseNoBody(errCode, resHeaders, response, httpCode = 200, log) {
        if (errCode && !response.headersSent) {
            return errorXMLResponse(errCode, response, log);
        }
        if (!response.headersSent) {
            return okHeaderResponse(resHeaders, response, httpCode, log);
        }
    },

    /**
     * @param {string} errCode - S3 error Code
     * @param {object} overrideHeaders - headers in this object override common
     * headers. These are extracted from the request object
     * @param {string} resHeaders - headers to be set for the response
     * @param {object} response - router's response object
     * @param {object} log - Werelogs logger
     * @return {object} - router's response object
     */
    responseContentHeaders(errCode, overrideHeaders, resHeaders, response,
        log) {
        if (errCode && !response.headersSent) {
            return errorXMLResponse(errCode, response, log);
        }
        if (!response.headersSent) {
            okContentHeadersResponse(overrideHeaders, resHeaders, response,
                log);
        }
        return response.end(() => {
            log.end().info('responded with content headers', {
                httpCode: response.statusCode,
            });
        });
    },

    /**
     * @param {string} errCode - S3 error Code
     * @param {object} overrideHeaders - headers in this object override common
     * headers. These are extracted from the request object
     * @param {string} resHeaders - headers to be set for the response
     * @param {array | null} dataLocations --
     *   - array of locations to get streams from sproxyd
     *   - null if no data for object and only metadata
     * @param {http.ServerResponse} response - response sent to the client
     * @param {object} log - Werelogs logger
     * @return {undefined}
     */
    responseStreamData(errCode, overrideHeaders,
            resHeaders, dataLocations, response, log) {
        if (errCode && !response.headersSent) {
            return errorXMLResponse(errCode, response, log);
        }
        if (!response.headersSent) {
            okContentHeadersResponse(overrideHeaders, resHeaders, response,
                log);
        }
        if (dataLocations === null) {
            return response.end(() => {
                log.end().info('responded with only metadata', {
                    httpCode: response.statusCode,
                });
            });
        }
        return routesUtils.responseStreamDataArray(dataLocations,
            response, log);
    },

    /**
     * @param {string[]} array - keys related to the object
     * @param {http.ServerResponse} response - response sent to the client
     * @param {object} log - Werelogs logger
     * @return {undefined}
     */
    responseStreamDataArray(array, response, log) {
        function finish(msg) {
            // Having the nulls here prevent the mock response object
            // in the test from interpreting the callback as data to write
            response.end(null, null, () => {
                log.end().info(msg, {
                    httpCode: response.statusCode,
                });
            });
        }

        function getPart(array, partNumber) {
            if (partNumber >= array.length) {
                return finish('responded with multi-part streamed content');
            }

            const key = array[partNumber];
            data.get(key, log, (err, readStream) => {
                const info = { partNumber, key };
                if (err) {
                    info.errorMessage = err.message;
                    log.error('unable to get object part', info);
                    return finish('unable to get object part');
                }

                readStream.on('end', () => {
                    log.debug('finished forwarding part', info);
                    process.nextTick(getPart, array, partNumber + 1);
                });

                readStream.on('data', (chunk) => {
                    info.chunkSize = chunk.length;
                    log.trace('forwarding chunk', info);
                    response.write(chunk);
                });
            });
        }

        getPart(array, 0);
    },
};

export default routesUtils;
