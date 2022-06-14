const assert = require('assert');
const { S3 } = require('aws-sdk');
const async = require('async');
const assertError = require('../../../../utilities/bucketTagging-util');

const getConfig = require('../support/config');

const bucket = 'policyputtaggingtestbucket';

const taggingNotUnique = {
    TagSet: [
        {
            Key: 'string',
            Value: 'string',
        },
        {
            Key: 'string',
            Value: 'stringaaaa',
        },
    ],
};

const validTagging = {
    TagSet: [
        {
            Key: 'key1',
            Value: 'value1',
        },
        {
            Key: 'key2',
            Value: 'value2',
        },
    ],
};

const validSingleTagging = {
    TagSet: [
        {
            Key: 'key1',
            Value: 'value1',
        },
    ],
};

const validEmptyTagging = {
    TagSet: [],
};

const taggingKeyNotValid = {
    TagSet: [
        {
            Key: 'stringaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaastringaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' +
                'astringaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaastringaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' +
                'stringaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            Value: 'string',
        },
        {
            Key: 'string',
            Value: 'stringaaaa',
        },
    ],
};

const taggingValueNotValid = {
    TagSet: [
        {
            Key: 'stringaaa',
            Value: 'string',
        },
        {
            Key: 'string',
            Value: 'stringaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaastringaaaaaaaaaaaaaaaaaaaaaa' +
                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaastringaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' +
                'aaaaaaastringaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaastringaaaaaaaaaaaaaaaaaa' +
                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaastringaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' +
                'aaaaaaaaaaastringaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaastringaaaaaaaaaaaaaa' +
                'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        },
    ],
};

describe('aws-sdk test put bucket tagging', () => {
    let s3;

    before(() => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
    });

    beforeEach(done => s3.createBucket({ Bucket: bucket }, done));

    afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

    it('should not add tag if tagKey not unique', done => {
        async.waterfall([
            next => s3.putBucketTagging({
                AccountId: s3.AccountId,
                Tagging: taggingNotUnique, Bucket: bucket,
            }, (err, res) => {
                next(err, res);
            }),
        ], (err) => {
            assertError(err, 'InvalidTag');
            done();
        });
    });

    it('should not add tag if tagKey not valid', done => {
        async.waterfall([
            next => s3.putBucketTagging({
                AccountId: s3.AccountId,
                Tagging: taggingKeyNotValid, Bucket: bucket,
            }, (err, res) => {
                next(err, res);
            }),
        ], (err) => {
            assertError(err, 'InvalidTag');
            done();
        });
    });

    it('should not add tag if tagValue not valid', done => {
        async.waterfall([
            next => s3.putBucketTagging({
                AccountId: s3.AccountId,
                Tagging: taggingValueNotValid, Bucket: bucket,
            }, (err, res) => {
                next(err, res);
            }),
        ], (err) => {
            assertError(err, 'InvalidTag');
            done();
        });
    });

    it('should add tag', done => {
        async.series([
            next => s3.putBucketTagging({
                AccountId: s3.AccountId,
                Tagging: validTagging, Bucket: bucket,
            }, (err, res) => {
                next(err, res);
            }),
            next => s3.getBucketTagging({
                AccountId: s3.AccountId,
                Bucket: bucket,
            }, (err, res) => {
                assert.deepStrictEqual(res, validTagging);
                next(err, res);
            }),
        ], (err) => {
            assert.ifError(err);
            done(err);
        });
    });

    it('should be able to put single tag', done => {
        async.series([
            next => s3.putBucketTagging({
                AccountId: s3.AccountId,
                Tagging: validSingleTagging, Bucket: bucket,
            }, (err, res) => {
                next(err, res, next);
            }),
            next => s3.getBucketTagging({
                AccountId: s3.AccountId,
                Bucket: bucket,
            }, (err, res) => {
                assert.deepStrictEqual(res, validSingleTagging);
                next(err, res);
            }),
        ], (err) => {
            assert.ifError(err);
            done(err);
        });
    });

    it('should be able to put empty tag array', done => {
        async.series([
            next => s3.putBucketTagging({
                AccountId: s3.AccountId,
                Tagging: validEmptyTagging, Bucket: bucket,
            }, next),
            next => s3.getBucketTagging({
                AccountId: s3.AccountId,
                Bucket: bucket,
            }, next),
        ], err => {
            assertError(err, 'NoSuchTagSet');
            done();
        });
    });

    it('should return accessDenied if expected bucket owner does not match', done => {
        async.waterfall([
            next => s3.putBucketTagging({ AccountId: s3.AccountId,
                Tagging: validEmptyTagging, Bucket: bucket, ExpectedBucketOwner: '944690102203' }, (err, res) => {
                next(err, res);
            }),
        ], (err) => {
            assertError(err, 'AccessDenied');
            done();
        });
    });

    it('should not return accessDenied if expected bucket owner matches', done => {
        async.series([
            next => s3.putBucketTagging({ AccountId: s3.AccountId,
                Tagging: validEmptyTagging, Bucket: bucket, ExpectedBucketOwner: s3.AccountId }, (err, res) => {
                next(err, res);
            }),
            next => s3.getBucketTagging({ AccountId: s3.AccountId, Bucket: bucket }, next),
        ], (err) => {
            assertError(err, 'NoSuchTagSet');
            done();
        });
    });

    it('should put 50 tags', done => {
        const tags = {
            TagSet: new Array(50).fill().map((el, index) => ({
                Key: `test_${index}`,
                Value: `value_${index}`,
            })),
        };
        s3.putBucketTagging({
            AccountId: s3.AccountId,
            Tagging: tags,
            Bucket: bucket,
            ExpectedBucketOwner: s3.AccountId
        }, err => {
            assert.ifError(err);
            done(err);
        });
    });

    it('should not put more than 50 tags', done => {
        const tags = {
            TagSet: new Array(51).fill().map((el, index) => ({
                Key: `test_${index}`,
                Value: `value_${index}`,
            })),
        };
        s3.putBucketTagging({
            AccountId: s3.AccountId,
            Tagging: tags,
            Bucket: bucket,
            ExpectedBucketOwner: s3.AccountId
        }, err => {
            assertError(err, 'BadRequest', done);
        });
    });
});
