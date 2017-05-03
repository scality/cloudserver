import assert from 'assert';
import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';
import config from '../../../../../lib/Config';

const bucket = 'buckettestmultiplebackendput';
const key = 'somekey';
const body = Buffer.from('I am a body', 'utf8');
const correctMD5 = 'be747eb4b75517bf6b3cf7c5fbb62f3a';

let bucketUtil;
let s3;
const itSkipIfE2E = process.env.S3_END_TO_END ? it.skip : it;
const describeSkipIfE2E = process.env.S3_END_TO_END ? it.skip : it;

describe('MultipleBackend put object', () => {
    withV4(sigCfg => {
        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            process.stdout.write('Creating bucket\n');
            return s3.createBucketAsync({ Bucket: bucket })
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        afterEach(() => {
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

        it('should return an error to put request without a valid bucket name',
            done => {
                s3.putObject({ Bucket: '', Key: key }, err => {
                    assert.notEqual(err, null,
                        'Expected failure but got success');
                    assert.strictEqual(err.code, 'MethodNotAllowed');
                    done();
                });
            });

        describe('with set location from "x-amz-meta-scal-' +
            'location-constraint" header', () => {
            it('should return an error to put request without a valid ' +
                'location constraint', done => {
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': 'fail-region' } };
                s3.putObject(params, err => {
                    assert.notEqual(err, null, 'Expected failure but got ' +
                        'success');
                    assert.strictEqual(err.code, 'InvalidArgument');
                    done();
                });
            });

            // SKIP because not mem location constraint in E2E.
            itSkipIfE2E('should put an object to mem', done => {
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': 'mem' },
                };
                s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                        `got error ${err}`);
                    s3.getObject({ Bucket: bucket, Key: key }, (err, res) => {
                        assert.strictEqual(err, null, 'Expected success, ' +
                        `got error ${err}`);
                        assert.strictEqual(res.ETag, `"${correctMD5}"`);
                        done();
                    });
                });
            });

            itSkipIfE2E('should put a 0-byte object to mem', done => {
                const params = { Bucket: bucket, Key: key,
                    Metadata: { 'scal-location-constraint': 'mem' },
                };
                s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                        `got error ${err}`);
                    s3.getObject({ Bucket: bucket, Key: key }, err => {
                        assert.strictEqual(err, null, 'Expected success, ' +
                        `got error ${err}`);
                        done();
                    });
                });
            });

            itSkipIfE2E('should put an object to file', done => {
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': 'file' },
                };
                s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                        `got error ${err}`);
                    s3.getObject({ Bucket: bucket, Key: key }, (err, res) => {
                        assert.strictEqual(err, null, 'Expected success, ' +
                        `got error ${err}`);
                        assert.strictEqual(res.ETag, `"${correctMD5}"`);
                        done();
                    });
                });
            });

            it('should put an object to AWS', done => {
                const params = { Bucket: bucket, Key: key,
                    Body: body,
                    Metadata: { 'scal-location-constraint': 'aws-test' } };
                s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                        `got error ${err}`);
                    s3.getObject({ Bucket: bucket, Key: key }, (err, res) => {
                        assert.strictEqual(err, null, 'Expected success, ' +
                        `got error ${err}`);
                        assert.strictEqual(res.ETag, `"${correctMD5}"`);
                        done();
                    });
                });
            });
        });
    });
});

describeSkipIfE2E('MultipleBackend put object based on bucket location', () => {
    withV4(sigCfg => {
        const params = { Bucket: bucket, Key: key, Body: body };
        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
        });

        afterEach(() => {
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

        it('should put an object to mem with no location header',
        done => {
            process.stdout.write('Creating bucket\n');
            return s3.createBucket({ Bucket: bucket,
                CreateBucketConfiguration: {
                    LocationConstraint: 'mem',
                },
            }, err => {
                assert.equal(err, null, `Error creating bucket: ${err}`);
                process.stdout.write('Putting object\n');
                return s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                        `got error ${JSON.stringify(err)}`);
                    s3.getObject({ Bucket: bucket, Key: key }, (err, res) => {
                        assert.strictEqual(err, null, 'Expected success, ' +
                        `got error ${JSON.stringify(err)}`);
                        assert.strictEqual(res.ETag, `"${correctMD5}"`);
                        done();
                    });
                });
            });
        });

        it('should put an object to file with no location header', done => {
            process.stdout.write('Creating bucket\n');
            return s3.createBucket({ Bucket: bucket,
                CreateBucketConfiguration: {
                    LocationConstraint: 'file',
                },
            }, err => {
                assert.equal(err, null, `Error creating bucket: ${err}`);
                process.stdout.write('Putting object\n');
                return s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                        `got error ${JSON.stringify(err)}`);
                    s3.getObject({ Bucket: bucket, Key: key }, (err, res) => {
                        assert.strictEqual(err, null, 'Expected success, ' +
                        `got error ${JSON.stringify(err)}`);
                        assert.strictEqual(res.ETag, `"${correctMD5}"`);
                        done();
                    });
                });
            });
        });

        it('should put an object to AWS with no location header', done => {
            process.stdout.write('Creating bucket\n');
            return s3.createBucket({ Bucket: bucket,
                CreateBucketConfiguration: {
                    LocationConstraint: 'aws-test',
                },
            }, err => {
                assert.equal(err, null, `Error creating bucket: ${err}`);
                process.stdout.write('Putting object\n');
                return s3.putObject(params, err => {
                    assert.equal(err, null, 'Expected success, ' +
                        `got error ${JSON.stringify(err)}`);
                    s3.getObject({ Bucket: bucket, Key: key }, (err, res) => {
                        assert.strictEqual(err, null, 'Expected success, ' +
                        `got error ${JSON.stringify(err)}`);
                        assert.strictEqual(res.ETag, `"${correctMD5}"`);
                        done();
                    });
                });
            });
        });
    });
});

describe('MultipleBackend put based on request endpoint',
() => {
    withV4(sigCfg => {
        before(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
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
                process.stdout.write(`Error in after: ${err}\n`);
                throw err;
            });
        });

        it('should create bucket in corresponding backend', done => {
            process.stdout.write('Creating bucket');
            const request = s3.createBucket({ Bucket: bucket });
            request.on('build', () => {
                request.httpRequest.body = '';
            });
            request.send(err => {
                assert.strictEqual(err, null, `Error creating bucket: ${err}`);
                s3.putObject({ Bucket: bucket, Key: key, Body: body }, err => {
                    assert.strictEqual(err, null, 'Expected succes, ' +
                        `got error ${JSON.stringify(err)}`);
                    const host = request.service.endpoint.hostname;
                    const endpoint = config.restEndpoints[host];
                    s3.getBucketLocation({ Bucket: bucket }, (err, data) => {
                        assert.strictEqual(err, null, 'Expected succes, ' +
                            `got error ${JSON.stringify(err)}`);
                        assert.strictEqual(data.LocationConstraint, endpoint);
                        s3.getObject({ Bucket: bucket, Key: key },
                        (err, res) => {
                            assert.strictEqual(err, null, 'Expected succes, ' +
                                `got error ${JSON.stringify(err)}`);
                            assert.strictEqual(res.ETag, `"${correctMD5}"`);
                            done();
                        });
                    });
                });
            });
        });
    });
});
