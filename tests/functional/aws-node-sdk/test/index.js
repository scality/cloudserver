const { S3 } = require('aws-sdk');
const assert = require('assert');
const getConfig = require('./support/config');

describe('S3 connect test', () => {
    const config = getConfig();
    const s3 = new S3(config);

    test('should list buckets', done => {
        s3.listBuckets((err, data) => {
            if (err) {
                done(err);
            }

            expect(data.Buckets).toBeTruthy();
            done();
        });
    });
});
