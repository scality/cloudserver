const assert = require('assert');
const { bucketPut } = require('../../../lib/api/bucketPut');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const bucketGetObjectLock = require('../../../lib/api/bucketGetObjectLock');
const bucketPutObjectLock = require('../../../lib/api/bucketPutObjectLock');

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'bucketname';

const bucketPutReq = {
    bucketName,
    headers: {
        host: `${bucketName}.s3.amazonaws.com`,
    },
    url: '/',
};

const testBucketPutReqWithObjLock = {
    bucketName,
    headers: {
        'host': `${bucketName}.s3.amazonaws.com`,
        'x-amz-bucket-object-lock-enabled': 'true',
    },
    url: '/',
};

function getObjectLockConfigRequest(bucketName, xml) {
    const request = {
        bucketName,
        headers: {
            'host': `${bucketName}.s3.amazonaws.com`,
            'x-amz-bucket-object-lock-enabled': 'true',
        },
        url: '/?object-lock',
    };
    if (xml) {
        request.post = xml;
    }
    return request;
}

function getObjectLockXml(mode, type, time) {
    const xml = {
        link: 'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
        objLockConfigOpen: '<ObjectLockConfiguration ',
        objLockConfigClose: '</ObjectLockConfiguration>',
        objectLockEnabled: '<ObjectLockEnabled>Enabled</ObjectLockEnabled>',
        ruleOpen: '<Rule><DefaultRetention>',
        ruleClose: '</DefaultRetention></Rule>',
    };
    const retentionMode = `<Mode>${mode}</Mode>`;
    const retentionTime = `<${type}>${time}</${type}>`;

    let xmlStr = `<?xml version="1.0" encoding="UTF-8"?>${xml.objLockConfigOpen}${xml.link}${xml.objectLockEnabled}`;

    // object lock is enabled and object lock configuration is set
    // eslint-disable-next-line
    if (arguments.length === 3) {
        xmlStr += xml.ruleOpen +
            retentionMode +
            retentionTime +
            xml.ruleClose;
    }
    xmlStr += xml.objLockConfigClose;
    return xmlStr;
}

describe('bucketGetObjectLock API', () => {
    before(done => bucketPut(authInfo, bucketPutReq, log, done));
    after(cleanup);

    it('should return ObjectLockConfigurationNotFoundError error if ' +
        'object lock is not enabled on the bucket', done => {
        const objectLockRequest = getObjectLockConfigRequest(bucketName);
        bucketGetObjectLock(authInfo, objectLockRequest, log, err => {
            assert.strictEqual(err.ObjectLockConfigurationNotFoundError, true);
            done();
        });
    });
});

describe('bucketGetObjectLock API', () => {
    before(cleanup);
    beforeEach(done => bucketPut(authInfo, testBucketPutReqWithObjLock, log, done));
    afterEach(cleanup);

    it('should return config without \'rule\' if object lock configuration ' +
        'not set on the bucket', done => {
        const objectLockRequest = getObjectLockConfigRequest(bucketName);
        bucketGetObjectLock(authInfo, objectLockRequest, log, (err, res) => {
            assert.ifError(err);
            const expectedXml = getObjectLockXml();
            assert.equal(expectedXml, res);
            done();
        });
    });

    describe('after object lock configuration has been put', () => {
        beforeEach(done => {
            const xml = getObjectLockXml('COMPLIANCE', 'Days', 90);
            const objectLockRequest = getObjectLockConfigRequest(bucketName, xml);
            bucketPutObjectLock(authInfo, objectLockRequest, log, err => {
                assert.ifError(err);
                done();
            });
        });

        it('should return object lock configuration XML', done => {
            const objectLockRequest = getObjectLockConfigRequest(bucketName);
            bucketGetObjectLock(authInfo, objectLockRequest, log, (err, res) => {
                assert.ifError(err);
                const expectedXml = getObjectLockXml('COMPLIANCE', 'Days', 90);
                assert.strictEqual(expectedXml, res);
                done();
            });
        });
    });
});
