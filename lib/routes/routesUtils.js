import { readySetStream } from 'ready-set-stream';

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
            if (headers[key] !== undefined) {
                try {
                    response.setHeader(key, headers[key]);
                } catch (e) {
                    log.warn('header can not be added ' +
                      'to the response', { header: headers[key],
                      error: e.stack, method: 'setCommonResponseHeaders' });
                }
            }
        });
    }
    response.setHeader('server', 'S3 Server');
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
 * @param {object} additionalHeaders -- additional headers to add to response
 * @return {object} response - response object with additional headers
 */
function okXMLResponse(xml, response, log, additionalHeaders) {
    log.trace('sending success xml response');
    setCommonResponseHeaders(additionalHeaders, response, log);
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
 * @param {array | undefined} range  - range in form of [start, end]
 * or undefined if no range header
 * @param {object} log - Werelogs logger
 * @return {object} response - modified response object
 */
function okContentHeadersResponse(overrideHeaders, resHeaders,
    response, range, log) {
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
    const httpCode = range ? 206 : 200;
    log.debug('response http code', { httpCode });
    response.writeHead(httpCode);
    return response;
}

const routesUtils = {
    /**
     * @param {string} errCode - S3 error Code
     * @param {string} xml - xml body as string conforming to S3's spec.
     * @param {object} response - router's response object
     * @param {object} log - Werelogs logger
     * @param {object} [additionalHeaders] - additionalHeaders to add
     * to response
     * @return {function} - error or success response utility
     */
    responseXMLBody(errCode, xml, response, log, additionalHeaders) {
        if (errCode && !response.headersSent) {
            return errorXMLResponse(errCode, response, log);
        }
        if (!response.headersSent) {
            return okXMLResponse(xml, response, log, additionalHeaders);
        }
        return undefined;
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
        return undefined;
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
            // Undefined added as an argument since need to send range to
            // okContentHeadersResponse in responseStreamData
            okContentHeadersResponse(overrideHeaders, resHeaders, response,
                undefined, log);
        }
        return response.end(() => {
            log.end().info('responded with content headers', {
                httpCode: response.statusCode,
            });
        });
    },

    /**
     * @param {array} dataLocations - all data locations
     * @param {array} outerRange - range from request
     * @return {array} parsedLocations - dataLocations filtered for
     * what needed and ranges added for particular parts as needed
     */
    setPartRanges(dataLocations, outerRange) {
        const parsedLocations = [];
        const begin = outerRange[0];
        const end = outerRange[1];
        // If have single location, do not need to break up range among parts
        // and might not have a start and size property
        // on the dataLocation (because might be pre- md-model-version 2),
        // so just set range as property
        if (dataLocations.length === 1) {
            const soleLocation = dataLocations[0];
            soleLocation.range = [begin, end];
            // If missing size, does not impact get range.
            // We modify size here in case this function is used for
            // object put part copy where will need size.
            // If pre-md-model-version 2, object put part copy will not
            // be allowed, so not an issue that size not modified here.
            if (dataLocations[0].size) {
                const partSize = parseInt(dataLocations[0].size, 10);
                soleLocation.size =
                    Math.min(partSize, end - begin + 1).toString();
            }
            parsedLocations.push(soleLocation);
            return parsedLocations;
        }
        // Range is inclusive of endpoint so need plus 1
        const max = end - begin + 1;
        let total = 0;
        for (let i = 0; i < dataLocations.length; i++) {
            if (total >= max) {
                break;
            }
            const partStart = parseInt(dataLocations[i].start, 10);
            const partSize = parseInt(dataLocations[i].size, 10);
            if (partStart + partSize <= begin) {
                continue;
            }
            if (partStart >= begin) {
                // If the whole part is in the range, just include it
                if (partSize + total <= max) {
                    const partWithoutRange = dataLocations[i];
                    partWithoutRange.size = partSize.toString();
                    parsedLocations.push(partWithoutRange);
                    total += partSize;
                    // Otherwise set a range limit on the part end
                    // and we're done
                } else {
                    const partWithRange = dataLocations[i];
                    // Need to subtract one from endPart since range
                    // includes endPart in byte count
                    const endPart = Math.min(partSize - 1, max - total - 1);
                    partWithRange.range = [0, endPart];
                    // modify size to be stored for object put part copy
                    partWithRange.size = (endPart + 1).toString();
                    parsedLocations.push(dataLocations[i]);
                    break;
                }
            } else {
                // Offset start (and end if necessary)
                const partWithRange = dataLocations[i];
                const startOffset = begin - partStart;
                // Use full remaining part if remaining partSize is less
                // than byte range we need to satisfy.  Or use byte range
                // we need to satisfy taking into account any startOffset
                const endPart = Math.min(partSize - 1,
                    max - total + startOffset - 1);
                partWithRange.range = [startOffset, endPart];
                // modify size to be stored for object put part copy
                partWithRange.size = (endPart - startOffset + 1).toString();
                parsedLocations.push(partWithRange);
                // Need to add byte back since with total we are counting
                // number of bytes while the endPart and startOffset
                // are in terms of range which include the endpoint
                total += endPart - startOffset + 1;
            }
        }
        return parsedLocations;
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
     * @param {array | undefined} range - range in format of [start, end]
     * if range header contained in request or undefined if not
     * @param {object} log - Werelogs logger
     * @return {undefined}
     */
    responseStreamData(errCode, overrideHeaders,
            resHeaders, dataLocations, response, range, log) {
        if (errCode && !response.headersSent) {
            return errorXMLResponse(errCode, response, log);
        }
        if (!response.headersSent) {
            okContentHeadersResponse(overrideHeaders, resHeaders, response,
                range, log);
        }
        if (dataLocations === null) {
            return response.end(() => {
                log.end().info('responded with only metadata', {
                    httpCode: response.statusCode,
                });
            });
        }
        const parsedLocations = range ? routesUtils
            .setPartRanges(dataLocations, range) : dataLocations.slice();
        response.on('finish', () => {
            log.end().info('responded with streamed content', {
                httpCode: response.statusCode,
            });
        });
        return readySetStream(parsedLocations, data.get, response, log);
    },

    /**
     * @param {object} err -- arsenal error object
     * @param {array} dataLocations --
     *   - array of locations to get streams from sproxyd
     * @param {http.ServerResponse} response - response sent to the client
     * @param {object} log - Werelogs logger
     * @return {undefined}
     */
    streamUserErrorPage(err, dataLocations, response, log) {
        setCommonResponseHeaders(null, response, log);
        response.writeHead(err.code, { 'Content-type': 'text/html' });
        response.on('finish', () => {
            log.end().info('responded with streamed content', {
                httpCode: response.statusCode,
            });
        });
        return readySetStream(dataLocations, data.get, response, log);
    },

    /**
     * @param {object} err - arsenal error object
     * @param {boolean} userErrorPageFailure - whether there was a failure
     * retrieving the user's error page
     * @param {string} bucketName - bucketName from request
     * @param {http.ServerResponse} response - response sent to the client
     * @param {object} log - Werelogs logger
     * @return {undefined}
     */
    errorHtmlResponse(err, userErrorPageFailure, bucketName, response, log) {
        log.trace('sending generic html error page',
            { err });
        setCommonResponseHeaders(null, response, log);
        response.writeHead(err.code, { 'Content-type': 'text/html' });
        const html = [];
        // response.statusMessage will provide standard message for status
        // code so much set response status code before creating html
        html.push(
            '<html>',
            '<head>',
            `<title>${err.code} ${response.statusMessage}</title>`,
            '</head>',
            '<body>',
            `<h1>${err.code} ${response.statusMessage}</h1>`,
            '<ul>',
            `<li id='code'>Code: ${err.message}</li>`,
            `<li id='message'>Message: ${err.description}</li>`
        );

        if (!userErrorPageFailure && bucketName) {
            html.push(`<li>BucketName: ${bucketName}</li>`);
        }
        html.push(
            `<li>RequestId: ${log.getSerializedUids()}</li>`,
            // AWS response contains HostId here.
            // TODO: consider adding
            '</ul>'
        );
        if (userErrorPageFailure) {
            html.push(
                '<h3>An Error Occurred While Attempting ',
                'to Retrieve a Custom ',
                'Error Document</h3>',
                '<ul>',
                `<li>Code: ${err.message}</li>`,
                `<li>Message: ${err.description}</li>`,
                '</ul>'
            );
        }
        html.push(
            '<hr>',
            '</body>',
            '</html>'
        );

        return response.end(html.join(''), 'utf8', () => {
            log.end().info('responded with error html', {
                httpCode: response.statusCode,
            });
        });
    },

    /**
     * redirectRequest - redirectRequest based on rule
     * @param {object} routingInfo - info for routing
     * @param {string} [routingInfo.hostName] - redirect host
     * @param {string} [routingInfo.protocol] - protocol for redirect
     * (http or https)
     * @param {number} [routingInfo.httpRedirectCode] - redirect http code
     * @param {string} [routingInfo.replaceKeyPrefixWith] - repalcement prefix
     * @param {string} [routingInfo.replaceKeyWith] - replacement key
     * @param {string} [routingInfo.prefixFromRule] - key prefix to be replaced
     * @param {string} objectKey - key name (may have been modified in
     * websiteGet api to include index document)
     * @param {boolean} encrypted - whether request was https
     * @param {object} response - response object
     * @param {object} log - Werelogs instance
     * @return {undefined}
     */
    redirectRequest(routingInfo, objectKey, encrypted, response, hostHeader,
        log) {
        const { hostName, protocol, httpRedirectCode, replaceKeyPrefixWith,
            replaceKeyWith, prefixFromRule } = routingInfo;

        const redirectProtocol = protocol || encrypted ? 'https' : 'http';
        const redirectCode = httpRedirectCode || 302;
        const redirectHostName = hostName || hostHeader;

        let redirectKey = objectKey;
        // will only have either replaceKeyWith defined or replaceKeyPrefixWith
        // defined.  not both and might have neither
        if (replaceKeyWith !== undefined) {
            redirectKey = replaceKeyWith;
        }
        if (replaceKeyPrefixWith !== undefined) {
            if (prefixFromRule !== undefined) {
                // if here with prefixFromRule defined, means that
                // passed condition
                // and objectKey starts with this prefix.  replace just first
                // instance in objectKey with the replaceKeyPrefixWith value
                redirectKey = objectKey.replace(prefixFromRule,
                    replaceKeyPrefixWith);
            } else {
                redirectKey = replaceKeyPrefixWith + objectKey;
            }
        }
        const redirectLocation =
            `${redirectProtocol}://${redirectHostName}/${redirectKey}`;
        log.end().info('redirecting request', {
            httpCode: redirectCode,
            redirectLocation: hostName,
        });
        response.writeHead(redirectCode, {
            Location: redirectLocation,
        });
        response.end();
        return undefined;
    },
};

export default routesUtils;
