const assert = require('assert');
const async = require('async');

const BucketUtility = require('../../lib/utility/bucket-util');

const {
    removeAllVersions,
    versioningEnabled,
} = require('../../lib/utility/versioning-util.js');

// This series of tests can only be enabled on an environment that has
// two Cloudserver instances, with one of them in null version
// compatibility mode. This is why they have to be explicitly enabled,
// which is done in a particular Integration test suite. This test
// suite makes the most sense in Integration because it tests the
// combination of Cloudserver requests to bucketd and the behavior of
// bucketd based on those requests.

const describeSkipIfNotExplicitlyEnabled =
      process.env.ENABLE_LEGACY_NULL_VERSION_COMPAT_TESTS ? describe : describe.skip;

describeSkipIfNotExplicitlyEnabled('legacy null version compatibility tests', () => {
    const bucketUtilCompat = new BucketUtility('default', {
        endpoint: 'http://127.0.0.1:8001',
    });
    const s3Compat = bucketUtilCompat.s3;
    const bucketUtil = new BucketUtility('default', {});
    const s3 = bucketUtil.s3;
    const bucket = `legacy-null-version-compat-${Date.now()}`;

    // In this series of tests, we first create a non-current null
    // version in legacy format (with "nullVersionId" field in the
    // master and no "isNull2" metadata attribute), by using the
    // Cloudserver endpoint that is configured with null version
    // compatibility mode enabled.
    beforeEach(done => async.series([
        next => s3Compat.createBucket({
            Bucket: bucket,
        }, next),
        next => s3Compat.putObject({
            Bucket: bucket,
            Key: 'obj',
            Body: 'nullbody',
        }, next),
        next => s3Compat.putBucketVersioning({
            Bucket: bucket,
            VersioningConfiguration: versioningEnabled,
        }, next),
        next => s3Compat.putObject({
            Bucket: bucket,
            Key: 'obj',
            Body: 'versionedbody',
        }, next),
    ], done));

    afterEach(done => {
        removeAllVersions({ Bucket: bucket }, err => {
            if (err) {
                return done(err);
            }
            return s3Compat.deleteBucket({ Bucket: bucket }, done);
        });
    });

    it('updating ACL of legacy null version with non-compat cloudserver', done => {
        async.series([
            next => s3.putObjectAcl({
                Bucket: bucket,
                Key: 'obj',
                VersionId: 'null',
                ACL: 'public-read',
            }, next),
            next => s3.getObjectAcl({
                Bucket: bucket,
                Key: 'obj',
                VersionId: 'null',
            }, (err, acl) => {
                assert.ifError(err);
                // check that we fetched the updated null version
                assert.strictEqual(acl.Grants.length, 2);
                next();
            }),
            next => s3.deleteObject({
                Bucket: bucket,
                Key: 'obj',
                VersionId: 'null',
            }, next),
            next => s3.listObjectVersions({
                Bucket: bucket,
            }, (err, listing) => {
                assert.ifError(err);
                // check that the null version has been correctly deleted
                assert(listing.Versions.every(version => version.VersionId !== 'null'));
                next();
            }),
        ], done);
    });

    it('updating tags of legacy null version with non-compat cloudserver', done => {
        const tagSet = [
            {
                Key: 'newtag',
                Value: 'newtagvalue',
            },
        ];
        async.series([
            next => s3.putObjectTagging({
                Bucket: bucket,
                Key: 'obj',
                VersionId: 'null',
                Tagging: {
                    TagSet: tagSet,
                },
            }, next),
            next => s3.getObjectTagging({
                Bucket: bucket,
                Key: 'obj',
                VersionId: 'null',
            }, (err, tagging) => {
                assert.ifError(err);
                assert.deepStrictEqual(tagging.TagSet, tagSet);
                next();
            }),
            next => s3.deleteObjectTagging({
                Bucket: bucket,
                Key: 'obj',
                VersionId: 'null',
            }, err => {
                assert.ifError(err);
                next();
            }),
            next => s3.getObjectTagging({
                Bucket: bucket,
                Key: 'obj',
                VersionId: 'null',
            }, (err, tagging) => {
                assert.ifError(err);
                assert.deepStrictEqual(tagging.TagSet, []);
                next();
            }),
            next => s3.deleteObject({
                Bucket: bucket,
                Key: 'obj',
                VersionId: 'null',
            }, next),
            next => s3.listObjectVersions({
                Bucket: bucket,
            }, (err, listing) => {
                assert.ifError(err);
                // check that the null version has been correctly deleted
                assert(listing.Versions.every(version => version.VersionId !== 'null'));
                next();
            }),
        ], done);
    });
});
