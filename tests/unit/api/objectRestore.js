const assert = require('assert');

const { bucketPut } = require('../../../lib/api/bucketPut');
const objectPut = require('../../../lib/api/objectPut');
const objectRestore = require('../../../lib/api/objectRestore');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const mdColdHelper = require('./utils/metadataMockColdStorage');
const DummyRequest = require('../DummyRequest');
const metadata = require('../metadataswitch');

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const namespace = 'default';
const bucketName = 'bucketname';
const objectName = 'objectName';
const postBody = Buffer.from('I am a body', 'utf8');
const restoreDays = 1;

const bucketPutRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
    actionImplicitDenies: false,
};

const putObjectRequest = new DummyRequest({
    bucketName,
    namespace,
    objectKey: objectName,
    headers: {},
    url: `/${bucketName}/${objectName}`,
}, postBody);

const objectRestoreXml = '<RestoreRequest ' +
    'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
    `<Days>${restoreDays}</Days>` +
    '<Tier>Standard</Tier>' +
    '</RestoreRequest>';

const objectRestoreXmlBulkTier = '<RestoreRequest ' +
    'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
    `<Days>${restoreDays}</Days>` +
    '<Tier>Bulk</Tier>' +
    '</RestoreRequest>';

const objectRestoreXmlExpeditedTier = '<RestoreRequest ' +
    'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
    `<Days>${restoreDays}</Days>` +
    '<Tier>Expedited</Tier>' +
    '</RestoreRequest>';

const objectRestoreRequest = requestXml => ({
        bucketName,
        objectKey: objectName,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        post: requestXml,
    });

describe('restoreObject API', () => {
    before(cleanup);

    afterEach(() => cleanup());

    it('should return InvalidObjectState error when object is not in cold storage', done => {
        bucketPut(authInfo, bucketPutRequest, log, err => {
            assert.ifError(err);
            objectPut(authInfo, putObjectRequest, undefined, log, err => {
                assert.ifError(err);
                objectRestore(authInfo, objectRestoreRequest(objectRestoreXml), log, err => {
                    assert.strictEqual(err.is.InvalidObjectState, true);
                    done();
                });
            });
        });
    });

    it('should return RestoreAlreadyInProgress error when object restore is already in progress', done => {
        mdColdHelper.putBucketMock(bucketName, null, () => {
            mdColdHelper.putObjectMock(bucketName, objectName, mdColdHelper.getArchiveOngoingRequestMD(), () => {
                objectRestore(authInfo, objectRestoreRequest(objectRestoreXml), log, err => {
                    assert.strictEqual(err.is.RestoreAlreadyInProgress, true);
                    done();
                });
            });
        });
    });

    it('should return NotImplemented error when object restore Tier is \'Bulk\'', done => {
        mdColdHelper.putBucketMock(bucketName, null, () => {
            mdColdHelper.putObjectMock(bucketName, objectName, mdColdHelper.getArchiveArchivedMD(), () => {
                objectRestore(authInfo, objectRestoreRequest(objectRestoreXmlBulkTier), log, err => {
                    assert.strictEqual(err.is.NotImplemented, true);
                    done();
                });
            });
        });
    });

    it('should return NotImplemented error when object restore Tier is \'Expedited\'', done => {
        mdColdHelper.putBucketMock(bucketName, null, () => {
            mdColdHelper.putObjectMock(bucketName, objectName, mdColdHelper.getArchiveArchivedMD(), () => {
                objectRestore(authInfo, objectRestoreRequest(objectRestoreXmlExpeditedTier), log, err => {
                    assert.strictEqual(err.is.NotImplemented, true);
                    done();
                });
            });
        });
    });

    it('should return Accepted and update objectMD ' +
        'while restoring an object from cold storage ' +
        'and the object doesn\'t have a restored copy in bucket', done => {
        const testStartTime = new Date(Date.now());
        mdColdHelper.putBucketMock(bucketName, null, () => {
            mdColdHelper.putObjectMock(bucketName, objectName, mdColdHelper.getArchiveArchivedMD(), () => {
                objectRestore(authInfo, objectRestoreRequest(objectRestoreXml), log, (err, statusCode) => {
                    assert.ifError(err);
                    assert.strictEqual(statusCode, 202);
                    metadata.getObjectMD(bucketName, objectName, {}, log, (err, md) => {
                        const testEndTime = new Date(Date.now());
                        assert.strictEqual(md.archive.restoreRequestedDays, restoreDays);
                        assert.strictEqual(testStartTime < md.archive.restoreRequestedAt < testEndTime, true);
                        done();
                        });
                });
            });
        });
    });

    it('should update the expiry time and return OK ' +
        'while restoring an object from cold storage ' +
        'and the object have a restored copy in bucket', done => {
        const testStartTime = new Date(Date.now());
        mdColdHelper.putBucketMock(bucketName, null, () => {
            mdColdHelper.putObjectMock(bucketName, objectName, mdColdHelper.getArchiveRestoredMD(), () => {
                objectRestore(authInfo, objectRestoreRequest(objectRestoreXml), log, (err, statusCode) => {
                    assert.ifError(err);
                    assert.strictEqual(statusCode, 200);
                    metadata.getObjectMD(bucketName, objectName, {}, log, (err, md) => {
                        const testEndTime = new Date(Date.now());
                        assert.strictEqual(md.archive.restoreRequestedDays, restoreDays);
                        assert.strictEqual(testStartTime < md.archive.restoreRequestedAt < testEndTime, true);
                            done();
                        });
                });
            });
        });
    });

    it('should return InvalidObjectState ' +
        'while restoring an expired restored object', () => {
        mdColdHelper.putBucketMock(bucketName, null, () => {
            mdColdHelper.putObjectMock(bucketName, objectName, mdColdHelper.getArchiveExpiredMD(), () => {
                objectRestore(authInfo, objectRestoreRequest(objectRestoreXml), log, err => {
                    assert.strictEqual(err.is.InvalidObjectState, true);
                });
            });
        });
    });
});
