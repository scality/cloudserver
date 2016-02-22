import xmlService from 'xml';

import S3ERRORS from './s3Errors.json';
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
 * @param {function} onEnd - A logging closure for the end of processing
 * @return {object} response - response object with additional headers
 */
function okHeaderResponse(headers, response, httpCode, log, onEnd) {
    log.trace('sending success header response');
    setCommonResponseHeaders(headers, response, log);
    log.debug('response http code', { httpCode });
    response.writeHead(httpCode);
    return response.end(() => {
        onEnd(log, 'responded to request', response.statusCode);
    });
}

/**
 * okXMLResponse - Response with XML body
 * @param {string} xml - XML body as string
 * @param {object} response - http response object
 * @param {object} log - Werelogs logger
 * @param {function} onEnd - A logging closure for the end of processing
 * @return {object} response - response object with additional headers
 */
function okXMLResponse(xml, response, log, onEnd) {
    log.trace('sending success xml response');
    setCommonResponseHeaders(null, response, log);
    response.writeHead(200, {
        'Content-type': 'application/xml'
    });
    log.debug('response http code', { httpCode: 200 });
    log.trace('xml response', { xml });
    return response.end(xml, 'utf8', () => {
        onEnd(log, 'responded with XML', response.statusCode);
    });
}

function errorXMLResponse(errCode, response, log, onEnd) {
    log.trace('sending error xml response', { errCode });
    const result = { xml: '', httpCode: 500 };
    /*
    <?xml version="1.0" encoding="UTF-8"?>
     <Error>
     <Code>NoSuchKey</Code>
     <Message>The resource you requested does not exist</Message>
     <Resource>/mybucket/myfoto.jpg</Resource>
     <RequestId>4442587FB7D0A2F9</RequestId>
     </Error>
     */
    const errObj = S3ERRORS[errCode] ? S3ERRORS[errCode]
        : S3ERRORS.InternalError;
    const errXMLObj = [
        {
            Error: [
                {
                    Code: errCode
                }, {
                    Message: errObj.description
                }, {
                    Resource: ''
                }, {
                    RequestId: ''
                }
            ]
        }
    ];
    result.xml = xmlService(errXMLObj, { declaration: { encoding: 'UTF-8' } });
    log.trace('error xml', { xml: result.xml });
    setCommonResponseHeaders(null, response, log);
    response.writeHead(errObj.httpCode, {
        'Content-type': 'application/xml'
    });
    log.debug('response http code', { httpCode: errObj.httpCode });
    return response.end(result.xml, 'utf8', () => {
        onEnd(log, 'responded with error XML', response.statusCode);
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
     * @param {function} onEnd - A logging closure for the end of processing
     * @return {function} - error or success response utility
     */
    responseXMLBody(errCode, xml, response, log, onEnd) {
        if (errCode && !response.headersSent) {
            return errorXMLResponse(errCode, response, log, onEnd);
        }
        if (!response.headersSent) {
            return okXMLResponse(xml, response, log, onEnd);
        }
    },

    /**
     * @param {string} errCode - S3 error Code
     * @param {string} resHeaders - headers to be set for the response
     * @param {object} response - router's response object
     * @param {number} httpCode - httpCode to set in response
     *   If none provided, defaults to 200.
     * @param {object} log - Werelogs logger
     * @param {function} onEnd - A logging closure for the end of processing
     * @return {function} - error or success response utility
     */
    responseNoBody(errCode, resHeaders, response, httpCode = 200, log, onEnd) {
        if (errCode && !response.headersSent) {
            return errorXMLResponse(errCode, response, log, onEnd);
        }
        if (!response.headersSent) {
            return okHeaderResponse(resHeaders, response, httpCode, log, onEnd);
        }
    },

    /**
     * @param {string} errCode - S3 error Code
     * @param {object} overrideHeaders - headers in this object override common
     * headers. These are extracted from the request object
     * @param {string} resHeaders - headers to be set for the response
     * @param {object} response - router's response object
     * @param {object} log - Werelogs logger
     * @param {function} onEnd - A logging closure for the end of processing
     * @return {object} - router's response object
     */
    responseContentHeaders(errCode, overrideHeaders, resHeaders, response,
        log, onEnd) {
        if (errCode && !response.headersSent) {
            return errorXMLResponse(errCode, response, log, onEnd);
        }
        if (!response.headersSent) {
            okContentHeadersResponse(overrideHeaders, resHeaders, response,
                log);
        }
        return response.end(() => {
            onEnd(log, 'responded with content headers', response.statusCode);
        });
    },

    /**
     * @param {string} errCode - S3 error Code
     * @param {object} overrideHeaders - headers in this object override common
     * headers. These are extracted from the request object
     * @param {string} resHeaders - headers to be set for the response
     * @param {stream.Readable} readStream - Stream interface to
     *                                       stream data in the response
     * @param {http.ServerResponse} response - response sent to the client
     * @param {object} log - Werelogs logger
     * @param {function} onEnd - A logging closure for the end of processing
     * @return {undefined}
     */
    responseStreamData(errCode, overrideHeaders,
            resHeaders, readStream, response, log, onEnd) {
        if (errCode && !response.headersSent) {
            return errorXMLResponse(errCode, response, log, onEnd);
        }
        if (!response.headersSent) {
            okContentHeadersResponse(overrideHeaders, resHeaders, response,
                log);
        }
        if (Array.isArray(readStream)) {
            return this.responseStreamDataArray(readStream, response,
                                                log, onEnd);
        }
        readStream.pipe(response, { end: false });
        readStream.on('end', function readStreamRes() {
            return response.end(() => {
                onEnd(log, 'responded with streamed content',
                      response.statusCode);
            });
        });
    },

    /**
     * @param {string[]} array - keys related to the object
     * @param {http.ServerResponse} response - response sent to the client
     * @param {object} log - Werelogs logger
     * @param {function} onEnd - A logging closure for the end of processing
     * @return {undefined}
     */
    responseStreamDataArray(array, response, log, onEnd) {
        function finish(msg) {
            response.end(() => {
                onEnd(log, msg, response.statusCode);
            });
        }

        function getPart(array, partNumber) {
            if (partNumber >= array.length) {
                return finish('responded with multi-part streamed content');
            }

            const key = array[partNumber];
            data.get(key, log, (err, readStream) => {
                const info = { partNumber, key, };
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

    /**
     * This function generates a closure that includes all the necessary
     * information from the request, in order to log all the relevant
     * information at the end of the request's processing
     *
     * @param {http.request} req - the http request sent to the server
     *
     * @returns {function} - A pre-computed closure that includes all the
     *                      necessary information about the request, to be used
     *                      as the last logging call before sending back a
     *                      response to the client.
     */
    onRequestEnd(req) {
        /**
         * @param {werelogs.RequestLogger} log - The werelogs request logger to
         *                                      use in order to log the output
         *                                      of the request's processing.
         * @param {string} msg - The string message to log
         * @param {number} code - The http code returned by the server to the
         *                        client for this request's processing.
         * @returns {undefined}
         */
        return function logReqEnd(log, msg, code) {
            log.end(msg, {
                clientIp: req.socket.remoteAddress,
                clientPort: req.socket.remotePort,
                httpMethod: req.method,
                httpURL: req.url,
                httpCode: code,
            });
        };
    }
};

export default routesUtils;
