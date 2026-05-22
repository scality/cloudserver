const assert = require('assert');
const {
    CreateBucketCommand,
    GetBucketLocationCommand,
    PutObjectCommand,
    HeadObjectCommand,
    GetObjectCommand,
} = require('@aws-sdk/client-s3');
const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const config = require('../../../config.json');
const specifiedEndpoint = `${config.transport}://127.0.0.3:8000`;
const bucket = 'testunknownendpoint';
const key = 'somekey';
const body = Buffer.from('I am a body', 'utf8');
const expectedETag = '"be747eb4b75517bf6b3cf7c5fbb62f3a"';

let bucketUtil;
let s3;

describe('Requests to ip endpoint not in config', () => {
    withV4(sigCfg => {
        before(() => {
            bucketUtil = new BucketUtility('default', { ...sigCfg, endpoint: specifiedEndpoint });
            s3 = bucketUtil.s3;
        });

        after(async () => {
            process.stdout.write('Emptying bucket\n');
            await bucketUtil.empty(bucket);
            process.stdout.write('Deleting bucket\n');
            await bucketUtil.deleteOne(bucket);
        });

        it(
            'should accept put bucket request ' + 'to IP address endpoint that is not in config using ' + 'path style',
            async () => {
                await s3.send(new CreateBucketCommand({ Bucket: bucket }));
            },
        );

        const itSkipIfE2E = process.env.S3_END_TO_END ? it.skip : it;
        // skipping in E2E since in E2E 127.0.0.3 resolving to
        // localhost which is in config. Once integration is using
        // different machines we can update this.
        itSkipIfE2E(
            'should show us-east-1 as bucket location since' +
                'IP address endpoint was not in config thereby ' +
                'defaulting to us-east-1',
            async () => {
                const res = await s3.send(new GetBucketLocationCommand({ Bucket: bucket }));
                assert.strictEqual(res.LocationConstraint, undefined);
            },
        );

        it(
            'should accept put object request ' +
                'to IP address endpoint that is not in config using ' +
                'path style and use the bucket location for the object',
            async () => {
                await s3.send(new PutObjectCommand({ Bucket: bucket, Key: key, Body: body }));
                await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
            },
        );

        it(
            'should accept get object request ' + 'to IP address endpoint that is not in config using ' + 'path style',
            async () => {
                const res = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
                assert.strictEqual(res.ETag, expectedETag);
            },
        );
    });
});
