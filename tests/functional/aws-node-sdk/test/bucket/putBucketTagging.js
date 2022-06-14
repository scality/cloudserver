const assert = require('assert');
const { errors } = require('arsenal');
const { S3 } = require('aws-sdk');
const async = require('async');

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

// Check for the expected error response code and status code.
function assertError(err, expectedErr, cb) {
    if (expectedErr === null) {
        assert.strictEqual(err, null, `expected no error but got '${err}'`);
    } else {
        assert.strictEqual(err.code, expectedErr, 'incorrect error response ' +
            `code: should be '${expectedErr}' but got '${err.code}'`);
        assert.strictEqual(err.statusCode, errors[expectedErr].code,
            'incorrect error status code: should be  ' +
            `${errors[expectedErr].code}, but got '${err.statusCode}'`);
    }
    cb();
}

describe('aws-sdk test put bucket tagging', () => {
    let s3;

    before(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
        return done();
    });

    beforeEach(done => s3.createBucket({ Bucket: bucket }, done));

    afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

    it('should not add tag if tagKey not unique', done => {
        async.waterfall([
            next => s3.putBucketTagging({ AccountId: s3.AccountId,
                Tagging: taggingNotUnique, Bucket: bucket }, (err, res) => {
                next(err, res);
            }),
        ], err => {
            assertError(err, 'InvalidTag', done);
        });
    });

    it('should not add tag if tagKey not valid', done => {
        async.waterfall([
            next => s3.putBucketTagging({ AccountId: s3.AccountId,
                Tagging: taggingKeyNotValid, Bucket: bucket }, (err, res) => {
                next(err, res);
            }),
        ], err => {
            assertError(err, 'InvalidTag', done);
        });
    });

    it('should not add tag if tagValue not valid', done => {
        async.waterfall([
            next => s3.putBucketTagging({ AccountId: s3.AccountId,
                Tagging: taggingValueNotValid, Bucket: bucket }, (err, res) => {
                next(err, res);
            }),
        ], err => {
            assertError(err, 'InvalidTag', done);
        });
    });

    it('should add tag', done => {
        async.waterfall([
            next => s3.putBucketTagging({ AccountId: s3.AccountId,
                Tagging: validTagging, Bucket: bucket }, (err, res) => {
                next(err, res);
            }), //TODO when getBucketTagging is done
        ], err => {
            assert.ifError(err);
            done(err);
        });
    });

    it('should be able to put single tag', done => {
        async.waterfall([
            next => s3.putBucketTagging({ AccountId: s3.AccountId,
                Tagging: validSingleTagging, Bucket: bucket }, (err, res) => {
                next(err, res);
            }), //TODO when getBucketTagging is done
        ], err => {
            assert.ifError(err);
            done(err);
        });
    });

    it('should be able to put empty tag array', done => {
        async.waterfall([
            next => s3.putBucketTagging({ AccountId: s3.AccountId,
                Tagging: validEmptyTagging, Bucket: bucket }, (err, res) => {
                next(err, res);
            }), //TODO when getBucketTagging is done
        ], err => {
            assert.ifError(err);
            done(err);
        });
    });

    it('should return accessDenied if expected bucket owner does not match', done => {
        async.waterfall([
            next => s3.putBucketTagging({ AccountId: s3.AccountId,
                Tagging: validEmptyTagging, Bucket: bucket, ExpectedBucketOwner: '944690102203' }, (err, res) => {
                next(err, res);
            }), //TODO when getBucketTagging is done
        ], err => {
            assertError(err, 'AccessDenied', done);
        });
    });

    it('should not return accessDenied if expected bucket owner matches', done => {
        async.waterfall([
            next => s3.putBucketTagging({ AccountId: s3.AccountId,
                Tagging: validEmptyTagging, Bucket: bucket, ExpectedBucketOwner: s3.AccountId }, (err, res) => {
                next(err, res);
            }), //TODO when getBucketTagging is done
        ], err => {
            assert.ifError(err);
            done(err);
        });
    });
});
