const assert = require('assert');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const provideRawOutput = require('../../lib/utility/provideRawOutput');
const { taggingTests } = require('../../lib/utility/tagging');
const genMaxSizeMetaHeaders
    = require('../../lib/utility/genMaxSizeMetaHeaders');

const bucket = 'bucket2putstuffin4324242';
const object = 'object2putstuffin';

function _checkError(err, code, statusCode) {
    expect(err).toBeTruthy();
    expect(err.code).toBe(code);
    expect(err.statusCode).toBe(statusCode);
}

function generateMultipleTagQuery(numberOfTag) {
    let tags = '';
    let and = '';
    for (let i = 0; i < numberOfTag; i++) {
        if (i !== 0) {
            and = '&';
        }
        tags = `key${i}=value${i}${and}${tags}`;
    }
    return tags;
}

describe('PUT object', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;

        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucketAsync({ Bucket: bucket })
            .catch(err => {
                process.stdout.write(`Error creating bucket: ${err}\n`);
                throw err;
            });
        });

        afterEach(() => {
            process.stdout.write('Emptying bucket');
            return bucketUtil.empty(bucket)
            .then(() => {
                process.stdout.write('Deleting bucket');
                return bucketUtil.deleteOne(bucket);
            })
            .catch(err => {
                process.stdout.write('Error in afterEach');
                throw err;
            });
        });

        test('should put an object and set the acl via query param', done => {
            const params = { Bucket: bucket, Key: 'key',
                ACL: 'public-read', StorageClass: 'STANDARD' };
            const url = s3.getSignedUrl('putObject', params);
            provideRawOutput(['-verbose', '-X', 'PUT', url,
                '--upload-file', 'uploadFile'], httpCode => {
                expect(httpCode).toBe('200 OK');
                s3.getObjectAcl({ Bucket: bucket, Key: 'key' },
                (err, result) => {
                    expect(err).toEqual(null);
                    assert.deepStrictEqual(result.Grants[1], { Grantee:
                    { Type: 'Group', URI:
                        'http://acs.amazonaws.com/groups/global/AllUsers',
                    }, Permission: 'READ' });
                    done();
                });
            });
        });

        test('should put an object with key slash', done => {
            const params = { Bucket: bucket, Key: '/' };
            s3.putObject(params, err => {
                expect(err).toEqual(null);
                done();
            });
        });

        test(
            'should return error if putting object w/ > 2KB user-defined md',
            done => {
                const metadata = genMaxSizeMetaHeaders();
                const params = { Bucket: bucket, Key: '/', Metadata: metadata };
                s3.putObject(params, err => {
                    expect(err).toBe(null);
                    // add one more byte to be over the limit
                    metadata.header0 = `${metadata.header0}${'0'}`;
                    s3.putObject(params, err => {
                        expect(err).toBeTruthy();
                        expect(err.code).toBe('MetadataTooLarge');
                        expect(err.statusCode).toBe(400);
                        done();
                    });
                });
            }
        );

        test('should return Not Implemented error for obj. encryption using ' +
            'AWS-managed encryption keys', done => {
            const params = { Bucket: bucket, Key: 'key',
                ServerSideEncryption: 'AES256' };
            s3.putObject(params, err => {
                expect(err.code).toBe('NotImplemented');
                done();
            });
        });

        test('should return Not Implemented error for obj. encryption using ' +
            'customer-provided encryption keys', done => {
            const params = { Bucket: bucket, Key: 'key',
                SSECustomerAlgorithm: 'AES256' };
            s3.putObject(params, err => {
                expect(err.code).toBe('NotImplemented');
                done();
            });
        });

        test('should return InvalidRedirectLocation if putting object ' +
        'with x-amz-website-redirect-location header that does not start ' +
        'with \'http://\', \'https://\' or \'/\'', done => {
            const params = { Bucket: bucket, Key: 'key',
                WebsiteRedirectLocation: 'google.com' };
            s3.putObject(params, err => {
                expect(err.code).toBe('InvalidRedirectLocation');
                expect(err.statusCode).toBe(400);
                done();
            });
        });
        describe('Put object with tag set', () => {
            taggingTests.forEach(taggingTest => {
                test(taggingTest.it, done => {
                    const key = encodeURIComponent(taggingTest.tag.key);
                    const value = encodeURIComponent(taggingTest.tag.value);
                    const tagging = `${key}=${value}`;
                    const params = { Bucket: bucket, Key: object,
                        Tagging: tagging };
                    s3.putObject(params, err => {
                        if (taggingTest.error) {
                            _checkError(err, taggingTest.error, 400);
                            return done();
                        }
                        expect(err).toEqual(null);
                        return s3.getObjectTagging({ Bucket: bucket,
                            Key: object }, (err, data) => {
                            expect(err).toEqual(null);
                            assert.deepStrictEqual(data.TagSet[0], {
                                Key: taggingTest.tag.key,
                                Value: taggingTest.tag.value });
                            done();
                        });
                    });
                });
            });
            test('should be able to put object with 10 tags', done => {
                const taggingConfig = generateMultipleTagQuery(10);
                s3.putObject({ Bucket: bucket, Key: object,
                    Tagging: taggingConfig }, err => {
                    expect(err).toEqual(null);
                    done();
                });
            });

            test('should be able to put an empty Tag set', done => {
                s3.putObject({ Bucket: bucket, Key: object,
                    Tagging: '',
                }, err => {
                    expect(err).toEqual(null);
                    done();
                });
            });

            test('should be able to put object with empty tags', done => {
                s3.putObject({ Bucket: bucket, Key: object,
                    Tagging: '&&&&&&&&&&&&&&&&&key1=value1' }, err => {
                    expect(err).toEqual(null);
                    done();
                });
            });

            test('should return BadRequest if putting more that 10 tags', done => {
                const taggingConfig = generateMultipleTagQuery(11);
                s3.putObject({ Bucket: bucket, Key: object,
                    Tagging: taggingConfig }, err => {
                    _checkError(err, 'BadRequest', 400);
                    done();
                });
            });

            test('should return InvalidArgument if using the same key twice', done => {
                s3.putObject({ Bucket: bucket, Key: object,
                    Tagging: 'key1=value1&key1=value2' }, err => {
                    _checkError(err, 'InvalidArgument', 400);
                    done();
                });
            });

            test('should return InvalidArgument if using the same key twice ' +
            'and empty tags', done => {
                s3.putObject({ Bucket: bucket, Key: object,
                    Tagging: '&&&&&&&&&&&&&&&&&key1=value1&key1=value2' },
                err => {
                    _checkError(err, 'InvalidArgument', 400);
                    done();
                });
            });

            test('should return InvalidArgument if tag with no key', done => {
                s3.putObject({ Bucket: bucket, Key: object,
                    Tagging: '=value1',
                }, err => {
                    _checkError(err, 'InvalidArgument', 400);
                    done();
                });
            });

            test('should return InvalidArgument putting object with ' +
            'bad encoded tags', done => {
                s3.putObject({ Bucket: bucket, Key: object, Tagging:
                'key1==value1' }, err => {
                    _checkError(err, 'InvalidArgument', 400);
                    done();
                });
            });
            test('should return InvalidArgument putting object tag with ' +
            'invalid characters: %', done => {
                const value = 'value1%';
                s3.putObject({ Bucket: bucket, Key: object, Tagging:
                `key1=${value}` }, err => {
                    _checkError(err, 'InvalidArgument', 400);
                    done();
                });
            });
        });
    });
});
