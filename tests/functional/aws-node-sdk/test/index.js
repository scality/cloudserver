import { S3 } from 'aws-sdk';
import assert from 'assert';
import getConfig from './support/config';

describe('S3 connect test', () => {
    const config = getConfig();
    const s3 = new S3(config);

    it('should list buckets', done => {
        s3.listBuckets((err, data) => {
            if (err) {
                done(err);
            }

            assert.ok(data.Buckets, 'should contain Buckets');
            done();
        });
    });
});
