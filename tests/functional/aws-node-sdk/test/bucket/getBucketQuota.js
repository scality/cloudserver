const AWS = require('aws-sdk');
const S3 = AWS.S3;
const assert = require('assert');
const getConfig = require('../support/config');
const sendRequest = require('../quota/tooling').sendRequest;

const bucket = 'getquotatestbucket';
const quota = { quota: 1000 };

describe('Test get bucket quota', () => {
    let s3;

    before(() => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
        AWS.config.update(config);
    });

    beforeEach(done => s3.createBucket({ Bucket: bucket }, done));

    afterEach(done => s3.deleteBucket({ Bucket: bucket }, done));

    it('should return the quota', done => {
        console.log(`AWS CONFIG: ${JSON.stringify(AWS.config)}`);
        sendRequest('POST', '127.0.0.1:8000', `/${bucket}/?quota=true`, JSON.stringify(quota), error => {
            if (error) {
                done(error);
                return;
            }
            sendRequest('GET', '127.0.0.1:8000', `/${bucket}/?quota=true`, '', (error, data) => {
                if (error) {
                    done(error);
                    return;
                }
                assert.strictEqual(data.GetBucketQuota.Name[0], bucket);
                assert.strictEqual(data.GetBucketQuota.Quota[0], '1000');
                done();
            });
        });
    });

    it('should return no such bucket error', done => {
        sendRequest('GET', '127.0.0.1:8000', '/test/?quota=true', '', err => {
            assert.strictEqual(err.Error.Code[0], 'NoSuchBucket');
            done();
        });
    });

    it('should return no such bucket quota', done => {
        sendRequest('DELETE', '127.0.0.1:8000', `/${bucket}/?quota=true`, '', err => {
            if (err) {
                done(err);
                return;
            }
            sendRequest('GET', '127.0.0.1:8000', `/${bucket}/?quota=true`, '', err => {
            assert.strictEqual(err.Error.Code[0], 'NoSuchBucketQuota');
            done();
            });
        });
    });
});
