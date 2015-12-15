import UUID from 'node-uuid';
import xmlService from 'xml';

import S3ERRORS from './s3Errors.json';

/**
 * setCommonResponseHeaders - Set HTTP response headers
 * @param {object} headers - key and value of new headers to add
 * @param {object} response - http response object
 * @return {object} response - response object with additional headers
 */
function setCommonResponseHeaders(headers, response) {
    if (headers && typeof headers === 'object') {
        Object.keys(headers).forEach((key) => {
            if (headers[key]) {
                response.setHeader(key, headers[key]);
            }
        });
    }

    // to be expanded in further implementation of logging of requests
    response.setHeader('x-amz-id-2', UUID.v4());
    response.setHeader('x-amz-request-id', UUID.v4());
    return response;
}
/**
 * okHeaderResponse - Response with only headers, no body
 * @param {object} headers - key and value of new headers to add
 * @param {object} response - http response object
 * @param {number} httpCode -- http response code
 * @return {object} response - response object with additional headers
 */
function okHeaderResponse(headers, response, httpCode) {
    setCommonResponseHeaders(headers, response);
    response.writeHead(httpCode);
    return response.end();
}

/**
 * okXMLResponse - Response with XML body
 * @param {string} xml - XML body as string
 * @param {object} response - http response object
 * @return {object} response - response object with additional headers
 */
function okXMLResponse(xml, response) {
    response.writeHead(200, {
        'Content-type': 'application/xml'
    });
    return response.end(xml, 'utf8');
}

function errorXMLResponse(errCode, response) {
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
            'Error': [
                {
                    'Code': errCode
                }, {
                    'Message': errObj.description
                }, {
                    'Resource': ''
                }, {
                    'RequestId': ''
                }
            ]
        }
    ];
    result.xml = xmlService(errXMLObj, { declaration: { encoding: 'UTF-8' }});
    response.writeHead(errObj.httpCode, {
        'Content-type': 'application/xml'
    });
    return response.end(result.xml, 'utf8');
}

/**
 * Modify response headers for an objectGet or objectHead request
 * @param {object} overrideHeaders - headers in this object override common
 * headers. These are extracted from the request object
 * @param {object} resHeaders - object with common response headers
 * @param {object} response - router's response object
 * @return {object} response - modified response object
 */
function okContentHeadersResponse(overrideHeaders, resHeaders, response) {
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

    setCommonResponseHeaders(addHeaders, response);
    response.writeHead(200);
    return response;
}

const routesUtils = {
    /**
     * @param {string} errCode - S3 error Code
     * @param {string} xml - xml body as string conforming to S3's spec.
     * @param {object} response - router's response object
     * @return {function} - error or success response utility
     */
    responseXMLBody(errCode, xml, response) {
        if (errCode) {
            return errorXMLResponse(errCode, response);
        }
        return okXMLResponse(xml, response);
    },

    /**
     * @param {string} errCode - S3 error Code
     * @param {string} resHeaders - headers to be set for the response
     * @param {object} response - router's response object
     * @param {number} httpCode - httpCode to set in response
     *   If none provided, defaults to 204.
     * @return {function} - error or success response utility
     */
    responseNoBody(errCode, resHeaders, response, httpCode = 204) {
        if (errCode) {
            return errorXMLResponse(errCode, response);
        }
        return okHeaderResponse(resHeaders, response, httpCode);
    },

    /**
     * @param {string} errCode - S3 error Code
     * @param {object} overrideHeaders - headers in this object override common
     * headers. These are extracted from the request object
     * @param {string} resHeaders - headers to be set for the response
     * @param {object} response - router's response object
     * @return {object} - router's response object
     */
    responseContentHeaders(errCode, overrideHeaders, resHeaders, response) {
        if (errCode) {
            return errorXMLResponse(errCode, response);
        }
        okContentHeadersResponse(overrideHeaders, resHeaders, response);
        return response.end();
    },

    /**
     * @param {string} errCode - S3 error Code
     * @param {object} overrideHeaders - headers in this object override common
     * headers. These are extracted from the request object
     * @param {string} resHeaders - headers to be set for the response
     * @param {object} readStream - instance of Node.js' Stream interface to
     * stream data in the response
     * @param {object} response - router's response object
     * @return {object} - router's response object
     */
    responseStreamData(errCode, overrideHeaders,
            resHeaders, readStream, response) {
        if (errCode) {
            return errorXMLResponse(errCode, response);
        }
        okContentHeadersResponse(overrideHeaders, resHeaders, response);
        readStream.pipe(response, { end: false });
        readStream.on('end', function readStreamRes() {
            return response.end();
        });
    }
};

export default routesUtils;
