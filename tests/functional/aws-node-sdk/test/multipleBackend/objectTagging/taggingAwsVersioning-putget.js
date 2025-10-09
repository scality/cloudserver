const async = require('async');
const {
    CreateBucketCommand,
    PutObjectCommand,
    DeleteObjectCommand,
    DeleteBucketCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');

const { removeAllVersions } = require('../../../lib/utility/versioning-util');
const {
    describeSkipIfNotMultiple,
    awsS3,
    awsBucket,
    awsLocation,
    enableVersioning,
    putNullVersionsToAws,
    putVersionsToAws,
    awsGetLatestVerId,
    tagging,
    genUniqID,
} = require('../utils');

const { putTaggingAndAssert, getTaggingAndAssert, delTaggingAndAssert,
    awsGetAssertTags } = tagging;
const bucket = `awsversioningtag${genUniqID()}`;
const someBody = 'teststring';

describeSkipIfNotMultiple('AWS backend object put/get tagging with versioning',
function testSuite() {
    this.timeout(120000);
    const tags = { key1: 'value1', key2: 'value2' };

    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        
        beforeEach(done => {
            const command = new CreateBucketCommand({
                Bucket: bucket,
                CreateBucketConfiguration: {
                    LocationConstraint: awsLocation,
                },
            });
            s3.send(command)
                .then(() => done())
                .catch(err => done(err));
        });

        afterEach(done => {
            removeAllVersions({ Bucket: bucket }, err => {
                if (err) {
                    return done(err);
                }
                return s3.send(new DeleteBucketCommand({ Bucket: bucket }))
                    .then(() => done()).catch(done);
            });
        });

        it('versioning not configured: should put/get a tag set on the ' +
        'latest version if no version is specified', done => {
            const key = `somekey-${genUniqID()}`;
            async.waterfall([
                next => {
                    const command = new PutObjectCommand({ Bucket: bucket, Key: key });
                    s3.send(command)
                        .then(data => next(null, data))
                        .catch(err => next(err));
                },
                (putData, next) => putTaggingAndAssert(s3, { bucket, key, tags,
                    expectedVersionId: false }, next),
                (versionId, next) => getTaggingAndAssert(s3, { bucket, key,
                    expectedTags: tags, expectedVersionId: false }, next),
                (versionId, next) => awsGetAssertTags({ key,
                    expectedTags: tags }, next),
            ], done);
        });

        it('versioning not configured: should put/get a tag set on a ' +
        'specific version if specified (null)', done => {
            const key = `somekey-${genUniqID()}`;
            async.waterfall([
                next => {
                    const command = new PutObjectCommand({ Bucket: bucket, Key: key });
                    s3.send(command)
                        .then(data => next(null, data))
                        .catch(err => next(err));
                },
                (putData, next) => putTaggingAndAssert(s3, { bucket, key, tags,
                    versionId: 'null', expectedVersionId: false }, next),
                (versionId, next) => getTaggingAndAssert(s3, { bucket, key,
                    versionId: 'null', expectedTags: tags,
                    expectedVersionId: false }, next),
                (versionId, next) => awsGetAssertTags({ key,
                    expectedTags: tags }, next),
            ], done);
        });

        it('versioning suspended: should put/get a tag set on the latest ' +
        'version if no version is specified', done => {
            const data = [undefined, 'test1', 'test2'];
            const key = `somekey-${genUniqID()}`;
            async.waterfall([
                next => putNullVersionsToAws(s3, bucket, key, data, next),
                (versionIds, next) => putTaggingAndAssert(s3, { bucket, key,
                    tags, expectedVersionId: 'null' }, next),
                (versionId, next) => getTaggingAndAssert(s3, { bucket, key,
                    expectedTags: tags, expectedVersionId: 'null' }, next),
                (versionId, next) => awsGetAssertTags({ key,
                    expectedTags: tags }, next),
            ], done);
        });

        it('versioning suspended: should put/get a tag set on a specific ' +
        'version (null)', done => {
            const key = `somekey-${genUniqID()}`;
            async.waterfall([
                next => putNullVersionsToAws(s3, bucket, key, [undefined],
                    next),
                (versionIds, next) => putTaggingAndAssert(s3, { bucket, key,
                    tags, versionId: 'null', expectedVersionId: 'null' }, next),
                (versionId, next) => getTaggingAndAssert(s3, { bucket, key,
                    versionId: 'null', expectedTags: tags,
                    expectedVersionId: 'null' }, next),
                (versionId, next) => awsGetAssertTags({ key,
                    expectedTags: tags }, next),
            ], done);
        });

        it('versioning enabled then suspended: should put/get a tag set on ' +
        'a specific (non-null) version if specified', done => {
            const key = `somekey-${genUniqID()}`;
            async.waterfall([
                next => enableVersioning(s3, bucket, next),
                next => {
                    const command = new PutObjectCommand({ Bucket: bucket, Key: key });
                    s3.send(command)
                        .then(data => next(null, data))
                        .catch(err => next(err));
                },
                (putData, next) => awsGetLatestVerId(key, '',
                    (err, awsVid) => next(err, putData.VersionId, awsVid)),
                (s3Vid, awsVid, next) => putNullVersionsToAws(s3, bucket, key,
                    [someBody], () => next(null, s3Vid, awsVid)),
                (s3Vid, awsVid, next) => putTaggingAndAssert(s3, { bucket, key,
                    tags, versionId: s3Vid, expectedVersionId: s3Vid }, () =>
                    next(null, s3Vid, awsVid)),
                (s3Vid, awsVid, next) => getTaggingAndAssert(s3, { bucket, key,
                    versionId: s3Vid, expectedTags: tags,
                    expectedVersionId: s3Vid }, () => next(null, awsVid)),
                (awsVid, next) => awsGetAssertTags({ key, versionId: awsVid,
                    expectedTags: tags }, next),
            ], done);
        });

        it('versioning enabled: should put/get a tag set on the latest ' +
        'version if no version is specified', done => {
            const key = `somekey-${genUniqID()}`;
            async.waterfall([
                next => enableVersioning(s3, bucket, next),
                next => {
                    const command = new PutObjectCommand({ Bucket: bucket, Key: key });
                    s3.send(command)
                        .then(data => next(null, data))
                        .catch(err => next(err));
                },
                (putData, next) => putTaggingAndAssert(s3, { bucket, key, tags,
                    expectedVersionId: putData.VersionId }, next),
                (versionId, next) => getTaggingAndAssert(s3, { bucket, key,
                    expectedTags: tags, expectedVersionId: versionId }, next),
                (versionId, next) => awsGetAssertTags({ key,
                    expectedTags: tags }, next),
            ], done);
        });

        it('versioning enabled: should put/get a tag set on a specific version',
        done => {
            const key = `somekey-${genUniqID()}`;
            async.waterfall([
                next => enableVersioning(s3, bucket, next),
                next => {
                    const command = new PutObjectCommand({ Bucket: bucket, Key: key });
                    s3.send(command)
                        .then(data => next(null, data))
                        .catch(err => next(err));
                },
                (putData, next) => putTaggingAndAssert(s3, { bucket, key, tags,
                    versionId: putData.VersionId,
                    expectedVersionId: putData.VersionId }, next),
                (versionId, next) => getTaggingAndAssert(s3, { bucket, key,
                    versionId, expectedTags: tags,
                    expectedVersionId: versionId }, next),
                (versionId, next) => awsGetAssertTags({ key,
                    expectedTags: tags }, next),
            ], done);
        });

        it('versioning enabled: should put/get a tag set on a specific version',
        done => {
            const key = `somekey-${genUniqID()}`;
            async.waterfall([
                next => enableVersioning(s3, bucket, next),
                next => {
                    const command = new PutObjectCommand({ Bucket: bucket, Key: key });
                    s3.send(command)
                        .then(data => next(null, data))
                        .catch(err => next(err));
                },
                (putData, next) => putTaggingAndAssert(s3, { bucket, key, tags,
                    versionId: putData.VersionId,
                    expectedVersionId: putData.VersionId }, next),
                (versionId, next) => delTaggingAndAssert(s3, { bucket, key,
                    versionId, expectedVersionId: versionId }, next),
                next => awsGetAssertTags({ key, expectedTags: {} }, next),
            ], done);
        });

        it('versioning enabled: should put/get a tag set on a specific ' +
        'version that is not the latest version', done => {
            const key = `somekey-${genUniqID()}`;
            async.waterfall([
                next => enableVersioning(s3, bucket, next),
                next => {
                    const command = new PutObjectCommand({ Bucket: bucket, Key: key });
                    s3.send(command)
                        .then(data => next(null, data))
                        .catch(err => next(err));
                },
                (putData, next) => awsGetLatestVerId(key, '',
                    (err, awsVid) => next(err, putData.VersionId, awsVid)),
                // put another version
                (s3Vid, awsVid, next) => {
                    const command = new PutObjectCommand({ 
                        Bucket: bucket,
                        Key: key, 
                        Body: someBody 
                    });
                    s3.send(command)
                        .then(() => next(null, s3Vid, awsVid))
                        .catch(err => next(err, s3Vid, awsVid));
                },
                (s3Vid, awsVid, next) => putTaggingAndAssert(s3, { bucket, key,
                    tags, versionId: s3Vid, expectedVersionId: s3Vid }, err =>
                    next(err, s3Vid, awsVid)),
                (s3Vid, awsVid, next) => getTaggingAndAssert(s3, { bucket, key,
                    versionId: s3Vid, expectedTags: tags,
                    expectedVersionId: s3Vid }, () => next(null, awsVid)),
                (awsVid, next) => awsGetAssertTags({ key, versionId: awsVid,
                    expectedTags: tags }, next),
            ], done);
        });

        it('versioning suspended then enabled: should put/get a tag set on ' +
        'a specific version (null) if specified', done => {
            const key = `somekey-${genUniqID()}`;
            async.waterfall([
                next => putNullVersionsToAws(s3, bucket, key, [undefined],
                    () => next()),
                next => awsGetLatestVerId(key, '', next),
                (awsVid, next) => putVersionsToAws(s3, bucket, key, [someBody],
                    () => next(null, awsVid)),
                (awsVid, next) => putTaggingAndAssert(s3, { bucket, key, tags,
                    versionId: 'null', expectedVersionId: 'null' },
                    () => next(null, awsVid)),
                (awsVid, next) => getTaggingAndAssert(s3, { bucket, key,
                    versionId: 'null', expectedTags: tags,
                    expectedVersionId: 'null' }, () => next(null, awsVid)),
                (awsVid, next) => awsGetAssertTags({ key, versionId: awsVid,
                    expectedTags: tags }, next),
            ], done);
        });

        it('should get tags for an object even if it was deleted from ' +
        'AWS directly (we rely on s3 metadata)',
        done => {
            const key = `somekey-${genUniqID()}`;
            async.waterfall([
                next => {
                    const command = new PutObjectCommand({ Bucket: bucket, Key: key });
                    s3.send(command)
                        .then(data => next(null, data))
                        .catch(err => next(err));
                },
                (putData, next) => awsGetLatestVerId(key, '', next),
                (awsVid, next) => putTaggingAndAssert(s3, { bucket, key, tags,
                    expectedVersionId: false }, () => next(null, awsVid)),
                (awsVid, next) => {
                    const command = new DeleteObjectCommand({ 
                        Bucket: awsBucket,
                        Key: key, 
                        VersionId: awsVid 
                    });
                    awsS3.send(command)
                        .then(data => next(null, data))
                        .catch(err => next(err));
                },
                (delData, next) => getTaggingAndAssert(s3, { bucket, key,
                    expectedTags: tags, expectedVersionId: false,
                    getObject: false }, next),
            ], done);
        });

        it('should return an ServiceUnavailable if trying to put ' +
        'tags from object that was deleted from AWS directly',
        done => {
            const key = `somekey-${genUniqID()}`;
            async.waterfall([
                next => {
                    const command = new PutObjectCommand({ Bucket: bucket, Key: key });
                    s3.send(command)
                        .then(data => next(null, data))
                        .catch(err => next(err));
                },
                (putData, next) => awsGetLatestVerId(key, '', next),
                (awsVid, next) => {
                    const command = new DeleteObjectCommand({ 
                        Bucket: awsBucket,
                        Key: key, 
                        VersionId: awsVid 
                    });
                    awsS3.send(command)
                        .then(data => next(null, data))
                        .catch(err => next(err));
                },
                (delData, next) => putTaggingAndAssert(s3, { bucket, key, tags,
                    expectedError: 'ServiceUnavailable' }, next),
            ], done);
        });

        it('should get tags for an version even if it was deleted from ' +
        'AWS directly (we rely on s3 metadata)',
        done => {
            const key = `somekey-${genUniqID()}`;
            async.waterfall([
                next => enableVersioning(s3, bucket, next),
                next => {
                    const command = new PutObjectCommand({ Bucket: bucket, Key: key });
                    s3.send(command)
                        .then(data => next(null, data))
                        .catch(err => next(err));
                },
                (putData, next) => awsGetLatestVerId(key, '',
                    (err, awsVid) => next(err, putData.VersionId, awsVid)),
                (s3Vid, awsVid, next) => putTaggingAndAssert(s3, { bucket, key,
                    tags, versionId: s3Vid, expectedVersionId: s3Vid },
                    () => next(null, s3Vid, awsVid)),
                (s3Vid, awsVid, next) => {
                    const command = new DeleteObjectCommand({ 
                        Bucket: awsBucket,
                        Key: key, 
                        VersionId: awsVid 
                    });
                    awsS3.send(command)
                        .then(() => next(null, s3Vid))
                        .catch(err => next(err, s3Vid));
                },
                (s3Vid, next) => getTaggingAndAssert(s3, { bucket, key,
                    versionId: s3Vid, expectedTags: tags,
                    expectedVersionId: s3Vid, getObject: false }, next),
            ], done);
        });

        it('should return an ServiceUnavailable if trying to put ' +
        'tags on version that was deleted from AWS directly',
        done => {
            const key = `somekey-${genUniqID()}`;
            async.waterfall([
                next => {
                    const command = new PutObjectCommand({ Bucket: bucket, Key: key });
                    s3.send(command)
                        .then(data => next(null, data))
                        .catch(err => next(err));
                },
                (putData, next) => awsGetLatestVerId(key, '',
                    (err, awsVid) => next(err, putData.VersionId, awsVid)),
                (s3Vid, awsVid, next) => {
                    const command = new DeleteObjectCommand({ 
                        Bucket: awsBucket,
                        Key: key, 
                        VersionId: awsVid 
                    });
                    awsS3.send(command)
                        .then(() => next(null, s3Vid))
                        .catch(err => next(err, s3Vid));
                },
                (s3Vid, next) => putTaggingAndAssert(s3, { bucket, key, tags,
                    versionId: s3Vid, expectedError:
                    'ServiceUnavailable' }, next),
            ], done);
        });
    });
});
