const assert = require('assert');
const { Readable } = require('stream');

const V4Transform = require('../../../lib/auth/streamingV4/V4Transform');
const { DummyRequestLogger } = require('../helpers');

const log = new DummyRequestLogger();
const streamingV4Params = { accessKey: 'accessKey1',
  signatureFromRequest: '2b8637632a997e06ee7b6c85d7' +
    '147d2025e8f04d4374f4d7d7320de1618c7509',
  region: 'us-east-1',
  scopeDate: '20170516',
  timestamp: '20170516T204738Z',
  credentialScope: '20170516/us-east-1/s3/aws4_request' };

class AuthMe extends Readable {
    constructor(chunks) {
        super();
        this._parts = chunks;
        this._index = 0;
    }

    _read() {
        this.push(this._parts[this._index]);
        this._index++;
    }

}

describe('V4Transform class', () => {
    it('should authenticate successfully', done => {
        const v4Transform = new V4Transform(streamingV4Params, log, err => {
            assert.strictEqual(err, null);
        });
        const chunks = [new Buffer('8;chunk-signature=' +
        '51d2511f7c6887907dff20474d8db67d557e5f515a6fa6a8466bb12f8833bcca\r\n' +
        'contents\r\n'), new Buffer('0;chunk-signature=' +
        'c0eac24b7ce72141ec077df9753db4cc8b7991491806689da0395c8bd0231e48\r\n'),
        null];
        const authMe = new AuthMe(chunks);
        authMe.pipe(v4Transform);
        v4Transform.on('finish', () => {
            done();
        });
    });

    it('should ignore data sent after final chunk', done => {
        const v4Transform = new V4Transform(streamingV4Params, log, err => {
            assert.strictEqual(err, null);
            done();
        });
        const chunks = [new Buffer('8;chunk-signature=' +
        '51d2511f7c6887907dff20474d8db67d557e5f515a6fa6a8466bb12f8833bcca\r\n' +
        'contents\r\n'), new Buffer('0;chunk-signature=' +
        'c0eac24b7ce72141ec077df9753db4cc8b7991491806689da0395c8bd0231e48\r\n'),
        new Buffer('\r\n'), null];
        const authMe = new AuthMe(chunks);
        authMe.pipe(v4Transform);
        v4Transform.on('finish', () => {
            done();
        });
    });
});
