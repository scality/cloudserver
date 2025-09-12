const assert = require('assert');
const crypto = require('crypto');
const sinon = require('sinon');

const { validateChecksumsNoChunking, ChecksumError, validateMethodChecksumNoChunking } = 
    require('../../../../../lib/api/apiUtils/integrity/validateChecksums');
const { errors: ArsenalErrors } = require('arsenal');
const { config } = require('../../../../../lib/Config');

describe('validateChecksumsNoChunking', () => {
    describe('with valid Content-MD5 header', () => {
        it('should return null when MD5 matches the body content', () => {
            const body = 'Hello, World!';
            const expectedMd5 = crypto.createHash('md5').update(body, 'utf8').digest('base64');
            const headers = {
                'content-md5': expectedMd5
            };

            const result = validateChecksumsNoChunking(headers, body);
            assert.strictEqual(result, null);
        });
    });

    describe('with MD5 mismatch', () => {
        it('should return MD5Mismatch error when checksums do not match', () => {
            const body = 'Hello, World!';
            const wrongMd5 = 'wrongchecksum123=';
            const expectedMd5 = crypto.createHash('md5').update(body, 'utf8').digest('base64');
            const headers = {
                'content-md5': wrongMd5
            };

            const result = validateChecksumsNoChunking(headers, body);
            assert.strictEqual(result.error, ChecksumError.MD5Mismatch);
            assert.strictEqual(result.details.calculated, expectedMd5);
            assert.strictEqual(result.details.expected, wrongMd5);
        });
    });

    describe('without Content-MD5 header', () => {
        it('should return MissingChecksum error when no content-md5 header is present', () => {
            const body = 'Hello, World!';
            const headers = {};

            const result = validateChecksumsNoChunking(headers, body);
            assert.strictEqual(result.error, ChecksumError.MissingChecksum);
            assert.strictEqual(result.details, null);
        });

        it('should return MissingChecksum error when headers object is undefined', () => {
            const body = 'Hello, World!';
            const headers = undefined;

            const result = validateChecksumsNoChunking(headers, body);
            assert.strictEqual(result.error, ChecksumError.MissingChecksum);
            assert.strictEqual(result.details, null);
        });
        
        it('should return MD5Mismatch error when content-md5 header is undefined', () => {
            const body = 'Hello, World!';
            const headers = {
                'content-type': 'application/json',
                'content-md5': undefined
            };
            const calculatedMD5 = crypto.createHash('md5').update(body, 'utf8').digest('base64');

            const result = validateChecksumsNoChunking(headers, body);
            assert.strictEqual(result.error, ChecksumError.MD5Mismatch);
            assert.strictEqual(result.details.calculated, calculatedMD5);
            assert.strictEqual(result.details.expected, undefined);
        });

        it('should return MD5Mismatch error when content-md5 header is null', () => {
            const body = 'Hello, World!';
            const headers = {
                'content-type': 'application/json',
                'content-md5': null
            };
            const calculatedMD5 = crypto.createHash('md5').update(body, 'utf8').digest('base64');

            const result = validateChecksumsNoChunking(headers, body);
            assert.strictEqual(result.error, ChecksumError.MD5Mismatch);
            assert.strictEqual(result.details.calculated, calculatedMD5);
            assert.strictEqual(result.details.expected, null);
        });

        it('should return MD5Mismatch error when content-md5 header is empty string', () => {
            const body = 'Hello, World!';
            const headers = {
                'content-type': 'application/json',
                'content-md5': ''
            };
            const calculatedMD5 = crypto.createHash('md5').update(body, 'utf8').digest('base64');

            const result = validateChecksumsNoChunking(headers, body);
            assert.strictEqual(result.error, ChecksumError.MD5Mismatch);
            assert.strictEqual(result.details.calculated, calculatedMD5);
            assert.strictEqual(result.details.expected, '');
        });
    });
});

describe('validateMethodChecksumNoChunking', () => {
    let sandbox;
    let originalIntegrityChecks;
    
    const supportedMethods = [
        'bucketPutACL',
        'bucketPutCors', 
        'bucketPutEncryption',
        'bucketPutLifecycle',
        'bucketPutNotification',
        'bucketPutObjectLock',
        'bucketPutPolicy',
        'bucketPutReplication',
        'bucketPutVersioning',
        'bucketPutWebsite',
        'multiObjectDelete',
        'objectPutACL',
        'objectPutLegalHold',
        'objectPutTagging',
        'objectPutRetention'
    ];

    beforeEach(() => {
        sandbox = sinon.createSandbox();
        originalIntegrityChecks = { ...config.integrityChecks };
    });

    afterEach(() => {
        sandbox.restore();
        config.integrityChecks = originalIntegrityChecks;
    });

    describe('when checksum mismatches', () => {
        supportedMethods.forEach(method => {
            it(`should return BadDigest error for ${method} when checksum mismatch`, () => {
                config.integrityChecks[method] = true;
                
                const body = 'Hello, World!';
                const wrongMd5 = 'wrongchecksum123=';
                const request = {
                    apiMethod: method,
                    headers: {
                        'content-md5': wrongMd5
                    }
                };
                const log = { debug: sandbox.stub() };

                const result = validateMethodChecksumNoChunking(request, body, log);
                
                assert.deepStrictEqual(result, ArsenalErrors.BadDigest, 'Expected BadDigest error');
                assert(log.debug.calledOnce);
            });
        });
    });

    describe('when no checksum is provided', () => {
        supportedMethods.forEach(method => {
            it(`should return null for ${method} when no checksum is provided`, () => {
                config.integrityChecks[method] = true;
                
                const body = 'Hello, World!';
                const request = {
                    apiMethod: method,
                    headers: {}
                };
                const log = { debug: sandbox.stub() };

                const result = validateMethodChecksumNoChunking(request, body, log);
                
                assert.strictEqual(result, null);
                assert(log.debug.notCalled);
            });
        });
    });

    describe('when checksum matches', () => {
        supportedMethods.forEach(method => {
            it(`should return null for ${method} when checksum matches`, () => {
                config.integrityChecks[method] = true;
                
                const body = 'Hello, World!';
                const correctMd5 = crypto.createHash('md5').update(body, 'utf8').digest('base64');
                const request = {
                    apiMethod: method,
                    headers: {
                        'content-md5': correctMd5
                    }
                };
                const log = { debug: sandbox.stub() };

                const result = validateMethodChecksumNoChunking(request, body, log);
                
                assert.strictEqual(result, null);
                assert(log.debug.notCalled);
            });
        });
    });

    describe('when method is disabled in config', () => {
        supportedMethods.forEach(method => {
            it(`should return null for ${method} when disabled, even with checksum mismatch`, () => {
                config.integrityChecks[method] = false;
                
                const body = 'Hello, World!';
                const wrongMd5 = 'wrongchecksum123=';
                const request = {
                    apiMethod: method,
                    headers: {
                        'content-md5': wrongMd5
                    }
                };
                const log = { debug: sandbox.stub() };

                const result = validateMethodChecksumNoChunking(request, body, log);
                
                assert.strictEqual(result, null);
                assert(log.debug.notCalled);
            });
        });
    });

    describe('when method is not in validation function mapping', () => {
        it('should return null for unsupported method even when enabled in config', () => {
            const unsupportedMethod = 'someUnsupportedMethod';
            config.integrityChecks[unsupportedMethod] = true;
            
            const body = 'Hello, World!';
            const wrongMd5 = 'wrongchecksum123=';
            const request = {
                apiMethod: unsupportedMethod,
                headers: {
                    'content-md5': wrongMd5
                }
            };
            const log = { debug: sandbox.stub() };

            const result = validateMethodChecksumNoChunking(request, body, log);
            
            assert.strictEqual(result, null);
            assert(log.debug.notCalled);
        });
    });

    describe('edge cases', () => {
        it('should return null when request has no apiMethod', () => {
            const body = 'Hello, World!';
            const request = {
                headers: {
                    'content-md5': 'wrongchecksum123='
                }
            };
            const log = { debug: sandbox.stub() };

            const result = validateMethodChecksumNoChunking(request, body, log);
            
            assert.strictEqual(result, null);
        });

        it('should return null when request apiMethod is undefined in config', () => {
            const body = 'Hello, World!';
            const request = {
                apiMethod: 'nonExistentMethod',
                headers: {
                    'content-md5': 'wrongchecksum123='
                }
            };
            const log = { debug: sandbox.stub() };

            const result = validateMethodChecksumNoChunking(request, body, log);
            
            assert.strictEqual(result, null);
        });
    });
});
