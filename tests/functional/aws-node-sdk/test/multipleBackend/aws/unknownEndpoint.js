const assert = require('assert');
const withV4 = require('../../support/withV4');
const BucketUtility = require('../../../lib/utility/bucket-util');
const config = require('../../../../config.json');
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
            bucketUtil = new BucketUtility('default', sigCfg);
            // change endpoint to endpoint with ip address
            // not in config
            bucketUtil.s3.config.endpoint = specifiedEndpoint;
            s3 = bucketUtil.s3;
        });

        after(() => {
            process.stdout.write('Emptying bucket\n');
            return bucketUtil.empty(bucket)
            .then(() => {
                process.stdout.write('Deleting bucket\n');
                return bucketUtil.deleteOne(bucket);
            })
            .catch(err => {
                process.stdout.write(`Error in afterEach: ${err}\n`);
                throw err;
            });
        });

        it('should accept put bucket request ' +
            'to IP address endpoint that is not in config using ' +
            'path style',
            done => {
                s3.createBucket({ Bucket: bucket }, err => {
                    assert.ifError(err);
                    done();
                });
            });

        const itSkipIfE2E = process.env.S3_END_TO_END ? it.skip : it;
        // skipping in E2E since in E2E 127.0.0.3 resolving to
        // localhost which is in config. Once integration is using
        // different machines we can update this.
        itSkipIfE2E('should show us-east-1 as bucket location since' +
            'IP address endpoint was not in config thereby ' +
            'defaulting to us-east-1',
            done => {
                s3.getBucketLocation({ Bucket: bucket },
                    (err, res) => {
                        assert.ifError(err);
                        // us-east-1 is returned as empty string
                        assert.strictEqual(res
                            .LocationConstraint, '');
                        done();
                    });
            });

        it('should accept put object request ' +
            'to IP address endpoint that is not in config using ' +
            'path style and use the bucket location for the object',
            done => {
                s3.putObject({ Bucket: bucket, Key: key, Body: body },
                    err => {
                        assert.ifError(err);
                        return s3.headObject({ Bucket: bucket, Key: key },
                            err => {
                                assert.ifError(err);
                                done();
                            });
                    });
            });

        it('should accept get object request ' +
            'to IP address endpoint that is not in config using ' +
            'path style',
            done => {
                s3.getObject({ Bucket: bucket, Key: key },
                    (err, res) => {
                        assert.ifError(err);
                        assert.strictEqual(res.ETag, expectedETag);
                        done();
                    });
            });
    });
});
