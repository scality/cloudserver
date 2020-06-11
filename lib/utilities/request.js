const url = require('url');
const http = require('http');
const https = require('https');
const HttpProxyAgent = require('http-proxy-agent');
const HttpsProxyAgent = require('https-proxy-agent');

const { jsutil } = require('arsenal');
const {
    proxyCompareUrl,
} = require('arsenal').storage.data.external.backendUtils;

const validVerbs = new Set(['HEAD', 'GET', 'POST', 'PUT', 'DELETE']);
const updateVerbs = new Set(['POST', 'PUT']);

/*
 * create a new header object from an existing header object. Similar keys
 * will be ignored if a value has been set for theirlower-cased form
 */
function createHeaders(headers)  {
    if (typeof headers !== 'object') {
        return {};
    }
    const out = {};
    Object.entries(headers).forEach(([key, value]) => {
        const lowKey = key.toLowerCase();
        if (out[lowKey] === undefined) {
            out[lowKey] = value;
        }
    });
    return out;
}

/**
 * @param {url.URL | string} endpoint -
 * @param {object} [options] -
 * @param {string} options.method - default: 'GET'
 * @param {object} options.headers- http headers
 * @param {boolean} options.json - if true, parse response body to json object
 * @param {function} callback - (error, response, body) => {}
 * @returns {object} request object
 */
function request(endpoint, options, callback) {
    if (!endpoint || typeof endpoint === 'function') {
        throw new Error('Missing target endpoint');
    }

    let cb;
    let opts = {};
    if (typeof options === 'function') {
        cb = jsutil.once(options);
    } else if (typeof options === 'object') {
        opts = JSON.parse(JSON.stringify(options)); // deep-copy
        if (typeof callback === 'function') {
            cb = jsutil.once(callback);
        }
    }

    if (typeof cb !== 'function') {
        throw new Error('Missing request callback');
    }

    if (!(endpoint instanceof url.URL || typeof endpoint === 'string')) {
        return cb(new Error(`Invalid URI ${endpoint}`));
    }

    if (!opts.method) {
        opts.method = 'GET';
    } else if (!validVerbs.has(opts.method)) {
        return cb(new Error(`Invalid Method ${opts.method}`));
    }

    let reqParams;
    if (typeof endpoint === 'string') {
        try {
           reqParams = url.parse(endpoint);
        } catch (error) {
            return cb(error);
        }
    } else {
        reqParams = url.parse(endpoint.href);
    }
    reqParams.method = opts.method;
    reqParams.headers = createHeaders(opts.headers || {});

    let request;
    if (reqParams.protocol === 'http:') {
        request = http;
        const httpProxy = process.env.HTTP_PROXY || process.env.http_proxy;
        if (!proxyCompareUrl(reqParams.hostname) && httpProxy) {
            reqParams.agent = new HttpProxyAgent(url.parse(httpProxy));
        }
    } else if (reqParams.protocol === 'https:') {
        request = https;
        const httpsProxy = process.env.HTTPS_PROXY || process.env.https_proxy;
        if (!proxyCompareUrl(reqParams.hostname) && httpsProxy) {
            reqParams.agent = new HttpsProxyAgent(url.parse(httpsProxy));
        }
    } else {
        return cb(new Error(`Invalid Protocol ${reqParams.protocol}`));
    }

    let data;
    if (opts.body) {
        if (typeof opts.body === 'object') {
            data = JSON.stringify(opts.body);
            if (!reqParams.headers['content-type']) {
                reqParams.headers['content-type'] = 'application/json';
            }
        } else {
            data = opts.body;
        }
        reqParams.headers['content-length'] = Buffer.byteLength(data);
    }

    const req = request.request(reqParams);
    req.on('error', cb);
    req.on('response', res => {
        const rawData  = [];
        res.on('data', chunk => { rawData.push(chunk); });
        res.on('end', () => {
            const data = rawData.join('');
            if (res.statusCode >= 400) {
                return cb(new Error(res.statusMessage), res, data);
            }

            if (opts.json && data) {
                try {
                    const parsed = JSON.parse(data);
                    return cb(null, res, parsed);
                } catch (err) {
                    // invalid json response
                    return cb(err, res, null);
                }
            }
            return cb(null, res, data);
        });
    });
    if (data !== undefined && updateVerbs.has(opts.method)) {
        req.write(data);
    }
    req.end();
    return req;
}

function _requestWrapper(method, url, options, callback) {
    let cb = callback;
    const opts = { method };

    if (typeof options === 'object') {
        Object.assign(opts, options);
    } else if (typeof options === 'function') {
        cb = options;
    }
    return request(url, opts, cb);
}

module.exports = {
    request,
    get: (url, opts, cb) => _requestWrapper('GET', url, opts, cb),
    post: (url, opts, cb) => _requestWrapper('POST', url, opts, cb),
    createHeaders,
};
