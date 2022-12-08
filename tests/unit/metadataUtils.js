const sinon = require('sinon'); // eslint-disable-line
const assert = require('assert');
const async = require('async');

const metadata = require('./metadataswitch');
const DummyRequest = require('./DummyRequest');
const { DummyRequestLogger, makeAuthInfo, versioningTestUtils } = require('./helpers');

const { bucketPut } = require('../../lib/api/bucketPut');
const objectPut = require('../../lib/api/objectPut');
const { addIsNonversionedBucket } = require('../../lib/metadata/metadataUtils');
const bucketPutVersioning = require('../../lib/api/bucketPutVersioning');

const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const bucketNameBase = 'testBucket';
const objectName = 'testObject';
const namespace = 'default';

const log = new DummyRequestLogger();
const any = sinon.match.any;
const original = metadata.client.getObject;

describe('addIsNonversionedBucket', () => {
    const testCases = [];
    [null, 'Enabled', 'Suspended'].forEach(bucketVersioning => {
        [{}, { foo: 'bar' }].forEach(options => {
            const name = `should pass the correct isNonversionedBucket flag when bucketVersioning is ${bucketVersioning}
                and the options passed into getObjectMD are ${JSON.stringify(options)}`;
            const testCase = {
                name,
                bucketVersioning,
                options,
            };
            testCases.push(testCase);
        });
    });

    let stub = null;
    beforeEach(() => {
        stub = sinon.stub(metadata.client, 'getObject');
        stub.callsFake(original);
    });

    afterEach(() => {
        stub.restore();
    });

    testCases.forEach((testCase, idx) => {
        it(testCase.name, done => {
            const { bucketVersioning, options } = testCase;
            const bucketName = bucketNameBase + idx;
            async.waterfall([
                next => {
                    const testBucketPutRequest = {
                        bucketName,
                        namespace,
                        headers: {},
                        url: `/${bucketName}`,
                    };

                    bucketPut(authInfo, testBucketPutRequest, log, err => {
                        assert.ifError(err);
                        next();
                    });
                },
                next => {
                    if (['Enabled', 'Suspended'].includes(testCase.bucketVersioning)) {
                        const versioningRequest = versioningTestUtils.createBucketPutVersioningReq(
                            bucketName, bucketVersioning,
                        );

                        bucketPutVersioning(authInfo, versioningRequest, log, err => {
                            assert.ifError(err);
                            next();
                        });
                    } else {
                        next();
                    }
                },
                next => {
                    const testPutObjectRequest = new DummyRequest({
                        bucketName,
                        headers: {},
                        url: `/${bucketName}/${objectName}`,
                        namespace,
                        objectKey: objectName,
                    }, Buffer.from('post body', 'utf8'));

                    objectPut(authInfo, testPutObjectRequest, undefined, log, err => {
                        assert.ifError(err);
                        next();
                    });
                },
                next => {
                    metadata.getBucket(bucketName, log, (err, bucket) => {
                        assert.ifError(err);
                        assert(bucket);
                        next(null, bucket);
                    });
                },
                (bucket, next) => {
                    // updated options contain the isNonversionedBucket flag.
                    const updatedOptions = addIsNonversionedBucket(options, bucket);
                    metadata.getObjectMD(bucketName, objectName, updatedOptions, log,
                        (err, md) => {
                            assert.ifError(err);
                            assert(md);
                            next();
                        });
                }], err => {
                assert.ifError(err);
                sinon.assert.calledOnce(metadata.client.getObject);
                if (testCase.bucketVersioning) {
                    sinon.assert.calledWith(metadata.client.getObject,
                        any, any, options, any, any);
                } else {
                    sinon.assert.calledWith(metadata.client.getObject,
                        any, any, Object.assign(options, { isNonversionedBucket: true }), any, any);
                }
                done();
            });
        });
    });
});
