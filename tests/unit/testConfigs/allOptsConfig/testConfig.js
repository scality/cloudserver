const fs = require('fs');
const assert = require('assert');

const basePath = __dirname;
const caPath = `${basePath}/caBundle.txt`;
const keyPath = `${basePath}/key.txt`;
const certPath = `${basePath}/cert.txt`;

process.env.S3_CONFIG_FILE = `${basePath}/config.json`;
const { ConfigObject } = require('../../../../lib/Config');
const config = new ConfigObject();

var originalEnv = process.env
delete process.env.HTTP_PROXY
delete process.env.HTTPS_PROXY
delete process.env.http_proxy
delete process.env.https_proxy
const config_no_proxy_from_env = new ConfigObject();
process.env = originalEnv

describe('Config with all possible options', () => {
    it('should include certFilePaths object', () => {
        const expectedObj1 = {
            ca: caPath,
            key: keyPath,
            cert: certPath,
        };
        assert.deepStrictEqual(expectedObj1, config.httpsPath);
        const expectedObj2 = {
            ca: fs.readFileSync(caPath, 'ascii'),
            key: fs.readFileSync(keyPath, 'ascii'),
            cert: fs.readFileSync(certPath, 'ascii'),
        };
        assert.deepStrictEqual(expectedObj2, config.https);
    });

    it('should include outboundProxy object', () => {
        const expectedObj1 = {
            url: 'http://test:8001',
            certs: {
                ca: fs.readFileSync(caPath, 'ascii'),
                key: fs.readFileSync(keyPath, 'ascii'),
                cert: fs.readFileSync(certPath, 'ascii'),
            },
        };
        assert.deepStrictEqual(expectedObj1, config_no_proxy_from_env.outboundProxy);
	const expectedObj2 = {
            url: process.env.HTTP_PROXY,
            certs: {
                ca: fs.readFileSync(caPath, 'ascii'),
                key: fs.readFileSync(keyPath, 'ascii'),
                cert: fs.readFileSync(certPath, 'ascii'),
            },
        };
        assert.deepStrictEqual(expectedObj2, config.outboundProxy);
    });
});
