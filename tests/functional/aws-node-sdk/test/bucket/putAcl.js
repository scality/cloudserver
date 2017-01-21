import assert from 'assert';
import { S3 } from 'aws-sdk';

import getConfig from '../support/config';

const bucket = `bucketacl-bucket-${Date.now()}`;

const grants = [];

// results in body of 589824 bytes
for (let i = 0; i < 100000; i ++) {
    grants.push({
        Grantee: {
            Type: 'CanonicalUser',
            DisplayName: 'STRING_VALUE',
            EmailAddress: 'STRING_VALUE',
            ID: 'STRING_VALUE',
        },
        Permission: 'READ',
    });
}

describe('aws-node-sdk test bucket put acl', () => {
    let s3;

    // setup test
    before(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new S3(config);
        s3.createBucket({ Bucket: bucket }, done);
    });

    // delete bucket after testing
    after(done => s3.deleteBucket({ Bucket: bucket }, done));

    const itSkipIfAWS = process.env.AWS_ON_AIR ? it.skip : it;
    itSkipIfAWS('should not accept xml body larger than 512 KB', done => {
        const params = {
            Bucket: bucket,
            AccessControlPolicy: {
                Grants: grants,
                Owner: {
                    DisplayName: 'STRING_VALUE',
                    ID: 'STRING_VALUE',
                },
            },
        };
        s3.putBucketAcl(params, error => {
            if (error) {
                assert.strictEqual(error.statusCode, 400);
                assert.strictEqual(
                    error.code, 'InvalidRequest');
                done();
            } else {
                done('accepted xml body larger than 512 KB');
            }
        });
    });
});
