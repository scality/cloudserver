const assert = require('assert');
const DummyRequest = require('./DummyRequest');
const MD5Sum = require('../../lib/utilities/MD5Sum');
const constants = require('../../constants');

function consume(stream) {
    stream.on('data', () => { });
}

function testMD5(payload, expectedMD5, done) {
    const dummy = new DummyRequest({}, payload);
    const md5summer = new MD5Sum();
    md5summer.on('hashed', () => {
        assert.strictEqual(md5summer.completedHash, expectedMD5);
        done();
    });
    dummy.pipe(md5summer);
    consume(md5summer);
}

describe('utilities.MD5Sum', () => {
    it('should work on empty request', done => {
        testMD5('', constants.emptyFileMd5, done);
    });

    it('should work on SAY GRRRR!!! request', done => {
        testMD5('SAY GRRRR!!!', '986eb4a201192e8b1723a42c1468fb4e', done);
    });

    it('should work on multiple MiB data stream', done => {
        /*
         * relies on a zero filled buffer and
         * split content in order to get multiple calls of _transform()
         * md5sum computed by hand with:
         * $ dd if=/dev/zero of=/dev/stdout bs=1M count=16 2>/dev/null | md5sum
         */
        const buffer = Buffer.alloc(4 * 1024 * 1024);
        testMD5([buffer, buffer, buffer, buffer],
                '2c7ab85a893283e98c931e9511add182', done);
    });
});
