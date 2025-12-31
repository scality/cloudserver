const { S3Client, ListBucketsCommand } = require('@aws-sdk/client-s3');
const assert = require('assert');
const getConfig = require('./support/config');

describe('S3 connect test', () => {
    const config = getConfig();
    const s3 = new S3Client(config);

    it('should list buckets', done => {
        s3.send(new ListBucketsCommand({}))
            .then(data => {
                assert.ok(data.Buckets, 'should contain Buckets');
                done();
            })
            .catch(err => {
                done(err);
            });
    });
});
