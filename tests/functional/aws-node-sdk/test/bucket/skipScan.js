const AWS = require('aws-sdk');
const async = require('async');
const assert = require('assert');

const getConfig = require('../support/config');

function cutAttributes(data) {
    const newContent = [];
    const newPrefixes = [];
    data.Contents.forEach(item => {
        newContent.push(item.Key);
    });
    /* eslint-disable no-param-reassign */
    data.Contents = newContent;
    data.CommonPrefixes.forEach(item => {
        newPrefixes.push(item.Prefix);
    });
    /* eslint-disable no-param-reassign */
    data.CommonPrefixes = newPrefixes;
    if (data.NextMarker === '') {
        /* eslint-disable no-param-reassign */
        delete data.NextMarker;
    }
    if (data.EncodingType === '') {
        /* eslint-disable no-param-reassign */
        delete data.EncodingType;
    }
    if (data.Delimiter === '') {
        /* eslint-disable no-param-reassign */
        delete data.Delimiter;
    }
}

const Bucket = `bucket-skip-scan-${Date.now()}`;

describe('Skip scan cases tests', () => {
    let s3;
    before(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new AWS.S3(config);
        s3.createBucket(
            { Bucket }, (err, data) => {
                if (err) {
                    done(err, data);
                }
                /* generating different prefixes every x > STREAK_LENGTH
                   to force the metadata backends to skip */
                const x = 120;
                async.timesLimit(500, 10,
                                 (n, next) => {
                                     const o = {};
                                     o.Bucket = Bucket;
                                     // eslint-disable-next-line
                                     o.Key = String.fromCharCode(65 + n / x) +
                                         '/' + n % x;
                                     o.Body = '';
                                     s3.putObject(o, (err, data) => {
                                         next(err, data);
                                     });
                                 }, done);
            });
    });
    after(done => {
        s3.listObjects({ Bucket }, (err, data) => {
            async.each(data.Contents, (o, next) => {
                s3.deleteObject({ Bucket, Key: o.Key }, next);
            }, () => {
                s3.deleteBucket({ Bucket }, done);
            });
        });
    });
    it('should find all common prefixes in one shot', done => {
        s3.listObjects({ Bucket, Delimiter: '/' }, (err, data) => {
            assert.strictEqual(err, null);
            cutAttributes(data);
            assert.deepStrictEqual(data, {
                IsTruncated: false,
                Marker: '',
                Contents: [],
                Delimiter: '/',
                Name: Bucket,
                Prefix: '',
                MaxKeys: 1000,
                CommonPrefixes: [
                    'A/',
                    'B/',
                    'C/',
                    'D/',
                    'E/',
                ],
            });
            done();
        });
    });
});
