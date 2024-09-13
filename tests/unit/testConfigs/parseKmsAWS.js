const assert = require('assert');
const sinon = require('sinon');
const fs = require('fs');
const path = require('path');

const { ConfigObject: Config } = require('../../../lib/Config');

describe('parseKmsAWS Function', () => {
    let configInstance;

    beforeEach(() => {
        configInstance = new Config();
    });

    it('should return an empty object if no kmsAWS config is provided', () => {
        const config = {};
        const result = configInstance._parseKmsAWS(config);
        assert.deepStrictEqual(result, {});
    });

    it('should throw an error if endpoint is not defined in kmsAWS', () => {
        const config = { kmsAWS: { ak: 'ak', sk: 'sk' } };
        assert.throws(() => configInstance._parseKmsAWS(config), 'endpoint must be defined');
    });

    it('should throw an error if ak is not defined in kmsAWS', () => {
        const config = { kmsAWS: { endpoint: 'https://example.com', sk: 'sk' } };
        assert.throws(() => configInstance._parseKmsAWS(config), 'ak must be defined');
    });

    it('should throw an error if sk is not defined in kmsAWS', () => {
        const config = { kmsAWS: { endpoint: 'https://example.com', ak: 'ak' } };
        assert.throws(() => configInstance._parseKmsAWS(config), 'sk must be defined');
    });

    it('should return the expected kmsAWS object when valid config is provided', () => {
        const config = {
            kmsAWS: {
                endpoint: 'https://example.com',
                ak: 'accessKey',
                sk: 'secretKey',
            },
        };
        const result = configInstance._parseKmsAWS(config);
        assert.deepStrictEqual(result, {
            endpoint: 'https://example.com',
            ak: 'accessKey',
            sk: 'secretKey',
        });
    });

    it('should include region if provided in the config', () => {
        const config = {
            kmsAWS: {
                endpoint: 'https://example.com',
                ak: 'accessKey',
                sk: 'secretKey',
                region: 'us-west-2',
            },
        };
        const result = configInstance._parseKmsAWS(config);
        assert.deepStrictEqual(result, {
            endpoint: 'https://example.com',
            ak: 'accessKey',
            sk: 'secretKey',
            region: 'us-west-2',
        });
    });

    it('should include tls configuration if provided', () => {
        const config = {
            kmsAWS: {
                endpoint: 'https://example.com',
                ak: 'accessKey',
                sk: 'secretKey',
                tls: {
                    rejectUnauthorized: true,
                    minVersion: 'TLSv1.2',
                    maxVersion: 'TLSv1.3',
                },
            },
        };
        const result = configInstance._parseKmsAWS(config);
        assert.deepStrictEqual(result, {
            endpoint: 'https://example.com',
            ak: 'accessKey',
            sk: 'secretKey',
            tls: {
                rejectUnauthorized: true,
                minVersion: 'TLSv1.2',
                maxVersion: 'TLSv1.3',
            },
        });
    });
});

describe('parseKmsAWS TLS section', () => {
    let readFileSyncStub;
    let configInstance;

    const mockCertifContent = Buffer.from('certificate');

    beforeEach(() => {
        configInstance = new Config();
        readFileSyncStub = sinon.stub(fs, 'readFileSync').returns(mockCertifContent);
    });

    afterEach(() => {
        readFileSyncStub.restore();
    });

    it('should throw an error if tls.rejectUnauthorized is not a boolean', () => {
        const config = {
            kmsAWS: {
                endpoint: 'https://example.com',
                ak: 'accessKey',
                sk: 'secretKey',
                tls: {
                    rejectUnauthorized: 'true', // Invalid type
                },
            },
        };

        assert.throws(() => configInstance._parseKmsAWS(config));
    });

    it('should throw an error if tls.minVersion is not a string', () => {
        const config = {
            kmsAWS: {
                endpoint: 'https://example.com',
                ak: 'accessKey',
                sk: 'secretKey',
                tls: {
                    minVersion: 1.2, // Invalid type
                },
            },
        };

        assert.throws(() => configInstance._parseKmsAWS(config), {
            message: 'bad config: KMS AWS TLS minVersion must be a string',
        });
    });

    it('should throw an error if tls.maxVersion is not a string', () => {
        const config = {
            kmsAWS: {
                endpoint: 'https://example.com',
                ak: 'accessKey',
                sk: 'secretKey',
                tls: {
                    maxVersion: 1.3, // Invalid type
                },
            },
        };

        assert.throws(() => configInstance._parseKmsAWS(config), {
            message: 'bad config: KMS AWS TLS maxVersion must be a string',
        });
    });

    it('should throw an error if tls.ca is not a string or an array', () => {
        const config = {
            kmsAWS: {
                endpoint: 'https://example.com',
                ak: 'accessKey',
                sk: 'secretKey',
                tls: {
                    ca: 12345, // Invalid type
                },
            },
        };

        assert.throws(() => configInstance._parseKmsAWS(config), {
            message: 'bad config: TLS file specification must be a string',
        });
    });

    it('should return an empty tls object if all tls fields are undefined', () => {
        const config = {
            kmsAWS: {
                endpoint: 'https://example.com',
                ak: 'accessKey',
                sk: 'secretKey',
                tls: {},
            },
        };

        const result = configInstance._parseKmsAWS(config);
        assert.deepStrictEqual(result.tls, {});
    });

    it('should load tls.ca as an array of files', () => {
        const config = {
            kmsAWS: {
                endpoint: 'http://example.com',
                ak: 'accessKey',
                sk: 'secretKey',
                tls: {
                    ca: ['/path/to/ca1.pem', '/path/to/ca2.pem'],
                },
            },
        };

        const result = configInstance._parseKmsAWS(config);

        assert.deepStrictEqual(result.tls.ca, [mockCertifContent, mockCertifContent]);
        assert(readFileSyncStub.calledTwice);
        assert(readFileSyncStub.calledWith('/path/to/ca1.pem'));
        assert(readFileSyncStub.calledWith('/path/to/ca2.pem'));
    });

    it('should load tls.cert as a single file', () => {
        const config = {
            kmsAWS: {
                endpoint: 'http://example.com',
                ak: 'accessKey',
                sk: 'secretKey',
                tls: {
                    cert: '/path/to/cert.pem',
                },
            },
        };

        const result = configInstance._parseKmsAWS(config);

        assert.deepStrictEqual(result.tls.cert, mockCertifContent);
        assert(readFileSyncStub.calledOnce);
        assert(readFileSyncStub.calledWith('/path/to/cert.pem'));
    });

    it('should load tls.key as a single file', () => {
        const config = {
            kmsAWS: {
                endpoint: 'http://example.com',
                ak: 'accessKey',
                sk: 'secretKey',
                tls: {
                    key: '/path/to/key.pem',
                },
            },
        };

        const result = configInstance._parseKmsAWS(config);

        assert.deepStrictEqual(result.tls.key, mockCertifContent);
        assert(readFileSyncStub.calledOnce);
        assert(readFileSyncStub.calledWith('/path/to/key.pem'));
    });

    it('should not load TLS files if tls is undefined', () => {
        const config = {
            kmsAWS: {
                endpoint: 'http://example.com',
                ak: 'accessKey',
                sk: 'secretKey',
            },
        };

        const result = configInstance._parseKmsAWS(config);

        assert.strictEqual(result.tls, undefined);
        assert(readFileSyncStub.notCalled);
    });

    it('should load tls.cert as a single file with relative path', () => {
        const certPath = 'path/to/cert.pem';
        const basePath = configInstance._basePath;
        const config = {
            kmsAWS: {
                endpoint: 'http://example.com',
                ak: 'accessKey',
                sk: 'secretKey',
                tls: {
                    cert: certPath,
                },
            },
        };

        const result = configInstance._parseKmsAWS(config);

        assert.deepStrictEqual(result.tls.cert, mockCertifContent);
        assert(readFileSyncStub.calledOnce);
        assert(readFileSyncStub.calledWith(path.join(basePath, certPath)));
    });

    it('should load tls.key, tls.cert, and tls.ca as arrays of files with relative paths', () => {
        const basePath = configInstance._basePath;

        const keyPaths = ['path/to/key1.pem', 'path/to/key2.pem'];
        const certPaths = ['path/to/cert1.pem', 'path/to/cert2.pem'];
        const caPaths = ['path/to/ca1.pem', 'path/to/ca2.pem'];

        const config = {
            kmsAWS: {
                endpoint: 'http://example.com',
                ak: 'accessKey',
                sk: 'secretKey',
                tls: {
                    key: keyPaths,
                    cert: certPaths,
                    ca: caPaths,
                },
            },
        };

        const result = configInstance._parseKmsAWS(config);

        assert.deepStrictEqual(result.tls.key, [mockCertifContent, mockCertifContent]);
        assert.deepStrictEqual(result.tls.cert, [mockCertifContent, mockCertifContent]);
        assert.deepStrictEqual(result.tls.ca, [mockCertifContent, mockCertifContent]);

        keyPaths.forEach((keyPath) => {
            assert(readFileSyncStub.calledWith(path.join(basePath, keyPath)));
        });

        certPaths.forEach((certPath) => {
            assert(readFileSyncStub.calledWith(path.join(basePath, certPath)));
        });

        caPaths.forEach((caPath) => {
            assert(readFileSyncStub.calledWith(path.join(basePath, caPath)));
        });

        assert(readFileSyncStub.callCount === (keyPaths.length + certPaths.length + caPaths.length));
    });
});
