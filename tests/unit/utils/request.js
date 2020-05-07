/* eslint-disable dot-notation */
const net = require('net');
const async = require('async');
const http = require('http');
const https = require('https');
const assert = require('assert');

const request = require('../../../lib/utilities/request');

const testCert = `-----BEGIN CERTIFICATE-----
MIIEXTCCAsWgAwIBAgIQGvSdUlFMc4oz/juP4sB7LDANBgkqhkiG9w0BAQsFADCB
hzEeMBwGA1UEChMVbWtjZXJ0IGRldmVsb3BtZW50IENBMS4wLAYDVQQLDCVjZW50
b3NAY29ubmVjdC5ub3ZhbG9jYWwgKENsb3VkIFVzZXIpMTUwMwYDVQQDDCxta2Nl
cnQgY2VudG9zQGNvbm5lY3Qubm92YWxvY2FsIChDbG91ZCBVc2VyKTAeFw0xOTA2
MDEwMDAwMDBaFw0zMDA1MDgyMTAwMjhaMFkxJzAlBgNVBAoTHm1rY2VydCBkZXZl
bG9wbWVudCBjZXJ0aWZpY2F0ZTEuMCwGA1UECwwlY2VudG9zQGNvbm5lY3Qubm92
YWxvY2FsIChDbG91ZCBVc2VyKTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC
ggEBAK0beLiu5Dg/BilCjdxmKw5K6rRMdZKNmAzN1RqBbI7rH8oRUWuSo+A/4f5K
31au9ydvy1QG3ftvnI2FMbnUL7bLfZMXqFuy5ZUtSmkfLSGWIgM7SJEOVlinM0/k
XPQltZNHAjPDCUySZDGR5/tlid3aBwNknFJt9aqknIUfB0LMOEqoYDYpOgQBnZfI
LjC4C97ICEEWA8nqQjy7tojjCdNsaUvkf3ph8F7ambUjp9T5qVeDhq+7aO9Crc/2
MfbLIFaYox1J5VHFPt6Hl7uWuzU2Wb8rpsoZ1I1WhCh/gd995NLpNAz0RdpFcA51
mkiG97wwZCXqceV4aq0g/o/Tr/MCAwEAAaNyMHAwDgYDVR0PAQH/BAQDAgWgMBMG
A1UdJQQMMAoGCCsGAQUFBwMBMAwGA1UdEwEB/wQCMAAwHwYDVR0jBBgwFoAUMbiC
i3C6yfO2eof84JvOG8CezJ4wGgYDVR0RBBMwEYIJbG9jYWxob3N0hwR/AAABMA0G
CSqGSIb3DQEBCwUAA4IBgQB88UTtn6wKOJV4VNyxLBq5E5qq9/pgqvpBdETNzx4U
6PTKSxxQ89YLYFVItEDsmf5Ww2tuehAFs9xRG3VnvFTljb4Jm2SeOqrA+QGUra9M
48z03RozREDtuMcA/xXnggPFwj1rvpjyYB49O8TkiTe6ZQkvlHqPC8KAB0+nc4zc
SVqrlHN2vaazDWyqsHy6JkwuDwMpj26FwxbQ0F0X3PcznwDppUN5fSuBf8gZYTDM
NcNVEQH6K/2a0xtU086fsKT5yw3Zn7LgSOK2moBgDAoor4CXbfUOmfJSsIK4HKKT
57V+85fUYdOFTfAp2f3ti+MkXTpBeeawDkLYqMYnwI1KJB1SgXUj2mSjxBObF79Y
dCrSF0+uFtOOGy/q8+kgCv3P72UD0iHaQlWJCmRszxAbLwUTxDcb3KrROt6J+uo2
c2IXTZZKtl8ws8cOMhy8FhGZ08R1waTv/AM9RW8Af9wKmE4gWZKwMEkIiwgXbIpP
XDzYCygidKezKa38lNvxr2g=
-----END CERTIFICATE-----`;

const testKey = `-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCtG3i4ruQ4PwYp
Qo3cZisOSuq0THWSjZgMzdUagWyO6x/KEVFrkqPgP+H+St9Wrvcnb8tUBt37b5yN
hTG51C+2y32TF6hbsuWVLUppHy0hliIDO0iRDlZYpzNP5Fz0JbWTRwIzwwlMkmQx
kef7ZYnd2gcDZJxSbfWqpJyFHwdCzDhKqGA2KToEAZ2XyC4wuAveyAhBFgPJ6kI8
u7aI4wnTbGlL5H96YfBe2pm1I6fU+alXg4avu2jvQq3P9jH2yyBWmKMdSeVRxT7e
h5e7lrs1Nlm/K6bKGdSNVoQof4HffeTS6TQM9EXaRXAOdZpIhve8MGQl6nHleGqt
IP6P06/zAgMBAAECggEAYJ8IzuyvbcWfxr/jNrXAKoYeYuyaY2atC9iHrfe3hD4w
zDTGYWjEF5MQrUNVTajzQwvfTdNXa9RsaYGCs9p/l2QVf1ihHC3Kf218Lxi7tBd8
fJlGc9Cw2WLZ/SnVyGCT7NShogqm0hqT2ic6eNPAFBhx+a7aohfGG60twBAFmX5A
MbUSo9/+o8Wfd8RYyohLKOvRS044gaIpYchu0VV/B0hTZIIG7Kf1RAb32NtDIi/9
d5oqjTSPYHLEjuAaGXU0HM1CwhdKwvuIwMi3PYGSETPaWXxrTD/oHYhejbJHb8JP
SSXUf9ZAVlQi7ugSaNPzRSYxv8adhN9F6XAGgdCgwQKBgQDGUiQWKwntCGHw1kdc
xQwUyRlYUXARQ8aSDmDjn2tj+5nxE2sSA+e08fYKxXnpgz9Z5kLdoOFmIM/p7VWg
iObuO4tKyOTVjeFKmgGAB6kuXRTo/78gJv3T3jMTAJSsejQAhUEGVi8SqGzi7/+b
5l5yF2gBc9SaszxJb7ey/b25+QKBgQDfdBIoVSyp+o7iU1mt5559DYP0KYrEXSNq
SCbrelTLH/c7+Q8rIKhLvuJKsHz0OXauOyTR4HkCdqVXn4W3LnaP+0xHAZqUPgJ/
MmbaC7WtohcAgz8koJ+gEZaFvqDPnWARvZEg4Zvoj2WLnTG1XshV/bxT/DzFOmAc
I94j5BzUSwKBgG3xo6fWfE4303QclrtLUgND6RUZMLOhizf1WKlX+8UV+qW3SihW
meGqMeiOyaM266v/Bxqu0cY4cosQ0+OHgd6YjAQNky8A1ODyt9ouZRAa8jipb193
vkfyawYh0Eo+BQ400XOd28LQNG0q992JvNorN7F2cWrB6q4bjQ9htAihAoGAOG6Q
01zHXdooUaIpKNEw5nYOWBBRukunUQUNxzRqy62z+5JnsUWbGx4G+kPeGrOUdGX9
Y3+kL+oU0a84zs1OJgiZ9+jAyCVs7gCllvWUGVixJHEA9lgWWA95CyfcZvJgu7o7
N6mifTYRuBOn5R0dzRG6iR9PnaOjeBfa1weZ/EcCgYAxW9DQxAKCkmLQT/Z1IKZm
BIw3g1Mc7lMQJUl0w80iXPjf77PtDsR2X+y49o5/DNbU4BV4AJZfkQEe5TjXo01U
Bk6Qaz6E4mH3iBT2upTuh0+AkKgyz8lh93idIWGZtaip1yRBZYU0I6WfFVfGxGNd
qpUgAEJjy9v59F76feVtJA==
-----END PRIVATE KEY-----`;

const postJson = { key: 'value' };
const postJsonStringified = JSON.stringify(postJson);
const postData = 'postrequest';

function respondWithError(req, res, code, message) {
    /* eslint-disable no-param-reassign */
    res.statusCode = code;
    res.statusMessage = message;
    res.end();
    /* eslint-enable no-param-reassign */
}

function respondWithValue(req, res, value) {
    value.forEach(val => res.write(val));
    res.end();
}

function handlePostRequest(req, res, expected) {
    let rawData = '';
    req.on('data', data => {
        rawData += data;
    });
    req.on('end', () => {
        if (rawData !== expected) {
            return respondWithError(req, res, 400, 'incorrect body value');
        }
        res.write('post completed');
        return res.end();
    });
}

function checkForHeaders(results, expected) {
    assert(Object.keys(expected).every(key => results[key] === expected[key]));
}

function testHandler(req, res) {
    switch (req.url) {
        case '/raw':
            return respondWithValue(req, res, ['bitsandbytes']);
        case '/json':
            return respondWithValue(req, res, [
                postJsonStringified.slice(0, 3),
                postJsonStringified.slice(3)
            ]);
        case '/post':
            if (req.method !== 'POST') return respondWithError(req, res, 405);
            return handlePostRequest(req, res, postData);
        case '/postjson':
            if (req.method !== 'POST') return respondWithError(req, res, 405);
            return handlePostRequest(req, res, postJsonStringified);
        case '/postempty':
            return handlePostRequest(req, res, '');
        default:
            return respondWithValue(req, res, ['default']);
    }
}

function createProxyServer(proto, targetHost, hostname, port, callback) {
    const target =  new URL(targetHost);
    let options = {};
    let serverType = http;
    if (proto === 'https') {
        options = { key: testKey, cert: testCert };
        serverType = https;
    }
    const proxy = serverType.createServer(options, (uReq, uRes) => {
        const req = http.request(target, res => res.pipe(uRes));
        req.end();
    });
    proxy.on('connect', (req, clnt) => {
        const svr = net.connect(target.port, target.hostname, () => {
            // handle http -> https
            clnt.write(
                `HTTP/${req.httpVersion} 200 Connection Established\r\n` +
                '\r\n'
            );
            svr.pipe(clnt);
            clnt.pipe(svr);
        });
    });
    proxy.listen(port, hostname, callback);
    return proxy;
}

function createTestServer(proto, hostname, port, handler, callback) {
    let options = {};
    let serverType = http;
    if (proto === 'https') {
        options = { key: testKey, cert: testCert };
        serverType = https;
    }
    const server = serverType.createServer(options,
                                              handler);
    server.on('error', err => {
        process.stdout.write(`https server: ${err.stack}\n`);
        process.exit(1);
    });
    server.listen(port, hostname, callback);
    return server;
}

[
    'http',
    'https',
].forEach(protocol => {
    describe(`test against ${protocol} server`, () => {
        const hostname = 'localhost';
        const testPort = 4242;
        const proxyPort = 8080;
        const sproxyPort = 8082;
        const host = `${protocol}://${hostname}:${testPort}`;
        const targetHost = `${protocol}://${hostname}:${8081}`;
        let server;
        let proxyServer;
        let sproxyServer;
        let proxyTarget;

        before(done => {
            process.env.NODE_TLS_REJECT_UNAUTHORIZED = 0;
            async.series([
                next => {
                    server = createTestServer(
                        protocol, hostname, testPort, testHandler, next);
                },
                next => {
                    proxyTarget = createTestServer(protocol, hostname, 8081,
                        (req, res) => res.end('proxyTarget'), next);
                },
                next => {
                    proxyServer = createProxyServer('http',
                        targetHost, hostname, proxyPort, next);
                },
                next => {
                    sproxyServer = createProxyServer('https',
                        targetHost, hostname, sproxyPort, next);
                },
            ], done);
        });

        after(done => {
            process.env.NODE_TLS_REJECT_UNAUTHORIZED = 1;
            async.series([
                next => server.close(next),
                next => proxyTarget.close(next),
                next => {
                    proxyServer.close();
                    sproxyServer.close();
                    next();
                },
            ], done);
        });

        afterEach(() => {
            process.env['http_proxy'] = '';
            process.env['https_proxy'] = '';
        });

        describe('request', () => {
            // tests:
            // http -> https
            // http -> http
            it('should set proxy agent to http proxy', done => {
                process.env['http_proxy'] = `http://localhost:${proxyPort}`;
                process.env['https_proxy'] = `http://localhost:${proxyPort}`;
                request.request(`${host}`, (err, res, body) => {
                    assert.ifError(err);
                    assert.equal(body, 'proxyTarget');
                    done();
                });
            });

            // tests:
            // https -> https
            // https -> http
            it('should set proxy agent to https proxy', done => {
                process.env['http_proxy'] = `https://localhost:${sproxyPort}`;
                process.env['https_proxy'] = `https://localhost:${sproxyPort}`;
                request.request(`${host}`, (err, res, body) => {
                    assert.ifError(err);
                    assert.equal(body, 'proxyTarget');
                    done();
                });
            });

            it('should return data', done => {
                request.request(`${host}/raw`, { json: false },
                    (err, res, body) => {
                        assert.ifError(err);
                        assert.equal(body, 'bitsandbytes');
                        done();
                    });
            });

            it('should convert output to json if "json" flag is set', done => {
                request.request(`${host}/json`, { json: true },
                    (err, res, body) => {
                        assert.ifError(err);
                        assert.deepStrictEqual(body, postJson);
                        done();
                    });
            });

            it('should set method to GET if it is missing', done => {
                const req = request.request(`${host}`,
                    (err, res) => {
                        assert.ifError(err);
                        assert.equal(res.statusCode, 200);
                        assert.equal(req.method, 'GET');
                        done();
                    });
            });

            it('should set headers', done => {
                const req = request.request(`${host}`, {
                        headers: {
                            'TEST-HEADERS-ONE': 'test-value-one',
                            'TEST-HEADERS-TWO': 'test-value-two',
                        },
                    },
                    (err, res) => {
                        assert.ifError(err);
                        assert.equal(res.statusCode, 200);
                        checkForHeaders(req.getHeaders(), {
                            'test-headers-one': 'test-value-one',
                            'test-headers-two': 'test-value-two',
                        });
                        done();
                    });
            });
        });

        describe('post', () => {
            it('should post data', done => {
                request.post(`${host}/post`, { body: postData },
                    (err, res, body) => {
                        assert.ifError(err);
                        assert.equal(res.statusCode, 200);
                        assert.equal(body, 'post completed');
                        done();
                    });
            });

            it('should post with json data', done => {
                request.post(`${host}/postjson`, { body: { key: 'value' } },
                    (err, res, body) => {
                        assert.ifError(err);
                        assert.equal(res.statusCode, 200);
                        assert.equal(body, 'post completed');
                        done();
                    });
            });

            it('should post with empty body', done => {
                request.post(`${host}/postempty`,
                    (err, res, body) => {
                        assert.ifError(err);
                        assert.equal(res.statusCode, 200);
                        assert.equal(body, 'post completed');
                        done();
                    });
            });

            it('should set content-type JSON if missing', done => {
                const req = request.post(`${host}`, {
                        body: postJson,
                        headers: { 'EXTRA': 'header' },
                    },
                    (err, res) => {
                        assert.ifError(err);
                        assert.equal(res.statusCode, 200);
                        checkForHeaders(req.getHeaders(), {
                            'content-type': 'application/json',
                            'content-length':
                                Buffer.byteLength(postJsonStringified),
                            'extra': 'header',
                        });
                        done();
                    });
            });

            it('should not overwrite existing content-type header value',
                done => {
                    const req = request.post(`${host}`, {
                        body: postJson,
                        headers: { 'Content-Type': 'text/plain' },
                    },
                    (err, res) => {
                        assert.ifError(err);
                        assert.equal(res.statusCode, 200);
                        checkForHeaders(req.getHeaders(), {
                            'content-type': 'text/plain',
                            'content-length':
                                Buffer.byteLength(postJsonStringified),
                        });
                        done();
                    });
                });
        });
    });
});

describe('utilities::request error handling', () => {
    it('should throw an error if arguments are missing', () =>  {
        assert.throws(request.request);
    });

    it('should throw an error if callback argument is missing', () =>  {
        assert.throws(() => request.request('http://test'));
    });

    it('should return an error for bad target endpoint', done => {
        request.request(123, err => {
            assert(err);
            done();
        });
    });

    it('should return an error for non http or https protocol', done => {
        request.request('ftp://test', err => {
            assert(err);
            done();
        });
    });

    it('should return an error for invalid verb', done => {
        request.request('http://test', { method: 'TEST' }, err => {
            assert(err);
            done();
        });
    });
});

describe('utilities::createHeaders', () => {
    it('should return an empty object if the argument is missing', () => {
        assert.deepStrictEqual(request.createHeaders(), {});
    });

    it('should return correct header object', () => {
        assert.deepStrictEqual(
            request.createHeaders({
                'CONTENT-TYPE': 'test/one',
                'content-type': 'test/two',
                'content-length': 1,
                'CONTENT-LENGTH': 1,
            }),
            {
                'content-type': 'test/one',
                'content-length': 1,
            }
        );
    });
});
