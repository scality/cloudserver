const async = require('async');
const assert = require('assert');
const crypto = require('crypto');

const { versioning } = require('arsenal');
const { parseString } = require('xml2js');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutVersioning = require('../../../lib/api/bucketPutVersioning');
const objectDelete = require('../../../lib/api/objectDelete');
const { multiObjectDelete } = require('../../../lib/api/multiObjectDelete');
const metadata = require('../metadataswitch');
const DummyRequest = require('../DummyRequest');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');

const versionIdUtils = versioning.VersionID;

const log = new DummyRequestLogger();
const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const bucketName = 'bucketname';
const objectName = 'objectName';

const testPutBucketRequest = new DummyRequest({
    bucketName,
    namespace,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
});
const testDeleteRequest = new DummyRequest({
    bucketName,
    namespace,
    objectKey: objectName,
    headers: {},
    url: `/${bucketName}/${objectName}`,
});

function _createBucketPutVersioningReq(status) {
    const request = {
        bucketName,
        headers: {
            host: `${bucketName}.s3.amazonaws.com`,
        },
        url: '/?versioning',
        query: { versioning: '' },
    };
    const xml = '<VersioningConfiguration ' +
    'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
    `<Status>${status}</Status>` +
    '</VersioningConfiguration>';
    request.post = xml;
    return request;
}

function _createMultiObjectDeleteRequest(numObjects) {
    const request = {
        bucketName,
        headers: {
            host: `${bucketName}.s3.amazonaws.com`,
        },
        url: '/?delete',
        query: { delete: '' },
    };
    const xml = [];
    xml.push('<?xml version="1.0" encoding="UTF-8"?>');
    xml.push('<Delete>');
    for (let i = 0; i < numObjects; i++) {
        xml.push('<Object>');
        xml.push(`<Key>${objectName}</Key>`);
        xml.push('</Object>');
    }
    xml.push('</Delete>');
    request.post = xml.join('');
    request.headers['content-md5'] = crypto.createHash('md5')
        .update(request.post, 'utf8').digest('base64');
    return request;
}

const enableVersioningRequest = _createBucketPutVersioningReq('Enabled');

const expectedAcl = {
    Canned: 'private',
    FULL_CONTROL: [],
    WRITE_ACP: [],
    READ: [],
    READ_ACP: [],
};

const undefHeadersExpected = [
    'cache-control',
    'content-disposition',
    'content-encoding',
    'expires',
];

describe('delete marker creation', () => {
    beforeEach(done => {
        cleanup();
        bucketPut(authInfo, testPutBucketRequest, log, err => {
            if (err) {
                return done(err);
            }
            return bucketPutVersioning(authInfo, enableVersioningRequest,
                log, done);
        });
    });

    afterEach(() => {
        cleanup();
    });

    function _assertDeleteMarkerMd(deleteResultVersionId, isLatest, callback) {
        const options = {
            versionId: isLatest ? undefined :
                versionIdUtils.decode(deleteResultVersionId),
        };
        return metadata.getObjectMD(bucketName, objectName, options, log,
            (err, deleteMarkerMD) => {
                assert.strictEqual(err, null);
                const mdVersionId = deleteMarkerMD.versionId;
                assert.strictEqual(deleteMarkerMD.isDeleteMarker, true);
                assert.strictEqual(versionIdUtils.encode(mdVersionId),
                    deleteResultVersionId);
                assert.strictEqual(deleteMarkerMD['content-length'], 0);
                assert.strictEqual(deleteMarkerMD.location, null);
                assert.deepStrictEqual(deleteMarkerMD.acl, expectedAcl);
                undefHeadersExpected.forEach(header => {
                    assert.strictEqual(deleteMarkerMD[header], undefined);
                });
                return callback();
            });
    }

    it('should create a delete marker if versioning enabled and deleting ' +
    'object without specifying version id', done => {
        objectDelete(authInfo, testDeleteRequest, log, (err, delResHeaders) => {
            if (err) {
                return done(err);
            }
            assert.strictEqual(delResHeaders['x-amz-delete-marker'], true);
            assert(delResHeaders['x-amz-version-id']);
            return _assertDeleteMarkerMd(delResHeaders['x-amz-version-id'],
                true, done);
        });
    });

    it('multi-object delete should create delete markers if versioning ' +
    'enabled and items do not have version id specified', done => {
        const testMultiObjectDeleteRequest =
            _createMultiObjectDeleteRequest(3);
        return multiObjectDelete(authInfo, testMultiObjectDeleteRequest, log,
            (err, xml) => {
                if (err) {
                    return done(err);
                }
                return parseString(xml, (err, parsedResult) => {
                    if (err) {
                        return done(err);
                    }
                    const results = parsedResult.DeleteResult.Deleted;
                    return async.forEach(results, (result, cb) => {
                        assert.strictEqual(result.Key[0], objectName);
                        assert.strictEqual(result.DeleteMarker[0], 'true');
                        assert(result.DeleteMarkerVersionId[0]);
                        _assertDeleteMarkerMd(result.DeleteMarkerVersionId[0],
                            false, cb);
                    }, err => done(err));
                });
            });
    });
});
