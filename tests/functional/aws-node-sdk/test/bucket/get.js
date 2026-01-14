const assert = require('assert');
const tv4 = require('tv4');
const { parseString } = require('xml2js');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const bucketSchema = require('../../schema/bucket');
const bucketSchemaV2 = require('../../schema/bucketV2');
const { generateToken, decryptToken } = require('../../../../../lib/api/apiUtils/object/continueToken');
const AWS = require('aws-sdk');
const { IAM } = AWS;
const getConfig = require('../support/config');
const { config } = require('../../../../../lib/Config');

const isVaultScality = config.backends.auth !== 'mem';
const internalPortBypassBP = config.internalPort;
const vaultHost = config.vaultd?.host || 'localhost';

const tests = [
    {
        name: 'return created objects in alphabetical order',
        objectPutParams: Bucket =>
            [
                { Bucket, Key: 'testB/' },
                { Bucket, Key: 'testB/test.json', Body: '{}' },
                { Bucket, Key: 'testA/' },
                { Bucket, Key: 'testA/test.json', Body: '{}' },
                { Bucket, Key: 'testA/test/test.json', Body: '{}' },
            ],
        listObjectParams: Bucket => ({ Bucket }),
        assertions: (data, Bucket) => {
            const keys = data.Contents.map(object => object.Key);
            // ETag should include quotes around value
            const emptyObjectHash =
                '"d41d8cd98f00b204e9800998ecf8427e"';
            assert.equal(data.Name, Bucket, 'Bucket name mismatch');
            assert.deepEqual(keys, [
                'testA/',
                'testA/test.json',
                'testA/test/test.json',
                'testB/',
                'testB/test.json',
            ], 'Bucket content mismatch');
            assert.deepStrictEqual(data.Contents[0].ETag,
                emptyObjectHash, 'Object hash mismatch');
        },
    },
    {
        name: 'return multiple common prefixes',
        objectPutParams: Bucket =>
            [
                { Bucket, Key: 'testB/' },
                { Bucket, Key: 'testB/test.json', Body: '{}' },
                { Bucket, Key: 'testA/' },
                { Bucket, Key: 'testA/test.json', Body: '{}' },
                { Bucket, Key: 'testA/test/test.json', Body: '{}' },
            ],
        listObjectParams: Bucket => ({ Bucket, Delimiter: '/' }),
        assertions: (data, Bucket) => {
            const prefixes = data.CommonPrefixes.map(cp => cp.Prefix);
            assert.equal(data.Name, Bucket, 'Bucket name mismatch');
            assert.deepEqual(prefixes, [
                'testA/',
                'testB/',
            ], 'Bucket content mismatch');
        },
    },
    {
        name: 'list objects with percentage delimiter',
        objectPutParams: Bucket =>
            [
                { Bucket, Key: 'testB%' },
                { Bucket, Key: 'testC%test.json', Body: '{}' },
                { Bucket, Key: 'testA%' },
            ],
        listObjectParams: Bucket => ({ Bucket, Delimiter: '%' }),
        assertions: data => {
            const prefixes = data.CommonPrefixes.map(cp => cp.Prefix);
            assert.deepEqual(prefixes, [
                'testA%',
                'testB%',
                'testC%',
            ], 'Bucket content mismatch');
        },
    },
    {
        name: 'list object titles with white spaces',
        objectPutParams: Bucket =>
            [
                { Bucket, Key: 'whiteSpace/' },
                { Bucket, Key: 'whiteSpace/one whiteSpace', Body: '{}' },
                { Bucket, Key: 'whiteSpace/two white spaces', Body: '{}' },
                { Bucket, Key: 'white space/' },
                { Bucket, Key: 'white space/one whiteSpace', Body: '{}' },
                { Bucket, Key: 'white space/two white spaces', Body: '{}' },
            ],
        listObjectParams: Bucket => ({ Bucket }),
        assertions: (data, Bucket) => {
            const keys = data.Contents.map(object => object.Key);
            assert.equal(data.Name, Bucket, 'Bucket name mismatch');
            assert.deepEqual(keys, [
                /* These object names are intentionally listed in a
                different order than they were created to additionally
                test that they are listed alphabetically. */
                'white space/',
                'white space/one whiteSpace',
                'white space/two white spaces',
                'whiteSpace/',
                'whiteSpace/one whiteSpace',
                'whiteSpace/two white spaces',
            ], 'Bucket content mismatch');
        },
    },
    {
        name: 'list object titles that contain special chars',
        objectPutParams: Bucket =>
            [
                { Bucket, Key: 'foo&<>\'"' },
                { Bucket, Key: '*asterixObjTitle/' },
                { Bucket, Key: '*asterixObjTitle/objTitleA', Body: '{}' },
                { Bucket, Key: '*asterixObjTitle/*asterixObjTitle',
                    Body: '{}' },
                { Bucket, Key: '.dotObjTitle/' },
                { Bucket, Key: '.dotObjTitle/objTitleA', Body: '{}' },
                { Bucket, Key: '.dotObjTitle/.dotObjTitle', Body: '{}' },
                { Bucket, Key: '(openParenObjTitle/' },
                { Bucket, Key: '(openParenObjTitle/objTitleA', Body: '{}' },
                { Bucket, Key: '(openParenObjTitle/(openParenObjTitle',
                    Body: '{}' },
                { Bucket, Key: ')closeParenObjTitle/' },
                { Bucket, Key: ')closeParenObjTitle/objTitleA', Body: '{}' },
                { Bucket, Key: ')closeParenObjTitle/)closeParenObjTitle',
                    Body: '{}' },
                { Bucket, Key: '!exclamationPointObjTitle/' },
                { Bucket, Key: '!exclamationPointObjTitle/objTitleA',
                    Body: '{}' },
                { Bucket, Key:
                  '!exclamationPointObjTitle/!exclamationPointObjTitle',
                    Body: '{}' },
                { Bucket, Key: '-dashObjTitle/' },
                { Bucket, Key: '-dashObjTitle/objTitleA', Body: '{}' },
                { Bucket, Key: '-dashObjTitle/-dashObjTitle', Body: '{}' },
                { Bucket, Key: '_underscoreObjTitle/' },
                { Bucket, Key: '_underscoreObjTitle/objTitleA', Body: '{}' },
                { Bucket, Key: '_underscoreObjTitle/_underscoreObjTitle',
                    Body: '{}' },
                { Bucket, Key: "'apostropheObjTitle/" },
                { Bucket, Key: "'apostropheObjTitle/objTitleA", Body: '{}' },
                { Bucket, Key: "'apostropheObjTitle/'apostropheObjTitle",
                    Body: '{}' },
                { Bucket, Key: 'çcedilleObjTitle' },
                { Bucket, Key: 'çcedilleObjTitle/objTitleA', Body: '{}' },
                { Bucket, Key: 'çcedilleObjTitle/çcedilleObjTitle',
                    Body: '{}' },
                { Bucket, Key: 'дcyrillicDObjTitle' },
                { Bucket, Key: 'дcyrillicDObjTitle/objTitleA', Body: '{}' },
                { Bucket, Key: 'дcyrillicDObjTitle/дcyrillicDObjTitle',
                    Body: '{}' },
                { Bucket, Key: 'ñenyeObjTitle' },
                { Bucket, Key: 'ñenyeObjTitle/objTitleA', Body: '{}' },
                { Bucket, Key: 'ñenyeObjTitle/ñenyeObjTitle', Body: '{}' },
                { Bucket, Key: '山chineseMountainObjTitle' },
                { Bucket, Key: '山chineseMountainObjTitle/objTitleA',
                    Body: '{}' },
                { Bucket, Key:
                  '山chineseMountainObjTitle/山chineseMountainObjTitle',
                    Body: '{}' },
                { Bucket, Key: 'àaGraveLowerCaseObjTitle' },
                { Bucket, Key: 'àaGraveLowerCaseObjTitle/objTitleA',
                    Body: '{}' },
                { Bucket,
                    Key: 'àaGraveLowerCaseObjTitle/àaGraveLowerCaseObjTitle',
                    Body: '{}' },
                { Bucket, Key: 'ÀaGraveUpperCaseObjTitle' },
                { Bucket, Key: 'ÀaGraveUpperCaseObjTitle/objTitleA',
                    Body: '{}' },
                { Bucket,
                    Key: 'ÀaGraveUpperCaseObjTitle/ÀaGraveUpperCaseObjTitle',
                    Body: '{}' },
                { Bucket, Key: 'ßscharfesSObjTitle' },
                { Bucket, Key: 'ßscharfesSObjTitle/objTitleA', Body: '{}' },
                { Bucket, Key: 'ßscharfesSObjTitle/ßscharfesSObjTitle',
                    Body: '{}' },
                { Bucket, Key: '日japaneseMountainObjTitle' },
                { Bucket, Key: '日japaneseMountainObjTitle/objTitleA',
                    Body: '{}' },
                { Bucket,
                    Key: '日japaneseMountainObjTitle/日japaneseMountainObjTitle',
                    Body: '{}' },
                { Bucket, Key: 'بbaArabicObjTitle' },
                { Bucket, Key: 'بbaArabicObjTitle/objTitleA', Body: '{}' },
                { Bucket, Key: 'بbaArabicObjTitle/بbaArabicObjTitle',
                    Body: '{}' },
                { Bucket,
                    Key: 'अadevanagariHindiObjTitle' },
                { Bucket,
                    Key: 'अadevanagariHindiObjTitle/objTitleA',
                    Body: '{}' },
                { Bucket,
                    Key: 'अadevanagariHindiObjTitle/अadevanagariHindiObjTitle',
                    Body: '{}' },
                { Bucket, Key: 'éeacuteLowerCaseObjTitle' },
                { Bucket, Key: 'éeacuteLowerCaseObjTitle/objTitleA',
                    Body: '{}' },
                { Bucket,
                    Key: 'éeacuteLowerCaseObjTitle/éeacuteLowerCaseObjTitle',
                    Body: '{}' },
            ],
        listObjectParams: Bucket => ({ Bucket }),
        assertions: (data, Bucket) => {
            const keys = data.Contents.map(object => object.Key);
            assert.equal(data.Name, Bucket, 'Bucket name mismatch');
            assert.deepEqual(keys, [
                /* These object names are intentionally listed in a
                different order than they were created to additionally
                test that they are listed alphabetically. */
                '!exclamationPointObjTitle/',
                '!exclamationPointObjTitle/!exclamationPointObjTitle',
                '!exclamationPointObjTitle/objTitleA',
                "'apostropheObjTitle/",
                "'apostropheObjTitle/'apostropheObjTitle",
                "'apostropheObjTitle/objTitleA",
                '(openParenObjTitle/',
                '(openParenObjTitle/(openParenObjTitle',
                '(openParenObjTitle/objTitleA',
                ')closeParenObjTitle/',
                ')closeParenObjTitle/)closeParenObjTitle',
                ')closeParenObjTitle/objTitleA',
                '*asterixObjTitle/',
                '*asterixObjTitle/*asterixObjTitle',
                '*asterixObjTitle/objTitleA',
                '-dashObjTitle/',
                '-dashObjTitle/-dashObjTitle',
                '-dashObjTitle/objTitleA',
                '.dotObjTitle/',
                '.dotObjTitle/.dotObjTitle',
                '.dotObjTitle/objTitleA',
                '_underscoreObjTitle/',
                '_underscoreObjTitle/_underscoreObjTitle',
                '_underscoreObjTitle/objTitleA',
                'foo&<>\'"',
                'ÀaGraveUpperCaseObjTitle',
                'ÀaGraveUpperCaseObjTitle/objTitleA',
                'ÀaGraveUpperCaseObjTitle/ÀaGraveUpperCaseObjTitle',
                'ßscharfesSObjTitle',
                'ßscharfesSObjTitle/objTitleA',
                'ßscharfesSObjTitle/ßscharfesSObjTitle',
                'àaGraveLowerCaseObjTitle',
                'àaGraveLowerCaseObjTitle/objTitleA',
                'àaGraveLowerCaseObjTitle/àaGraveLowerCaseObjTitle',
                'çcedilleObjTitle',
                'çcedilleObjTitle/objTitleA',
                'çcedilleObjTitle/çcedilleObjTitle',
                'éeacuteLowerCaseObjTitle',
                'éeacuteLowerCaseObjTitle/objTitleA',
                'éeacuteLowerCaseObjTitle/éeacuteLowerCaseObjTitle',
                'ñenyeObjTitle',
                'ñenyeObjTitle/objTitleA',
                'ñenyeObjTitle/ñenyeObjTitle',
                'дcyrillicDObjTitle',
                'дcyrillicDObjTitle/objTitleA',
                'дcyrillicDObjTitle/дcyrillicDObjTitle',
                'بbaArabicObjTitle',
                'بbaArabicObjTitle/objTitleA',
                'بbaArabicObjTitle/بbaArabicObjTitle',
                'अadevanagariHindiObjTitle',
                'अadevanagariHindiObjTitle/objTitleA',
                'अadevanagariHindiObjTitle/अadevanagariHindiObjTitle',
                '山chineseMountainObjTitle',
                '山chineseMountainObjTitle/objTitleA',
                '山chineseMountainObjTitle/山chineseMountainObjTitle',
                '日japaneseMountainObjTitle',
                '日japaneseMountainObjTitle/objTitleA',
                '日japaneseMountainObjTitle/日japaneseMountainObjTitle',
            ], 'Bucket content mismatch');
        },
    },
    {
        name: 'list objects with special chars in CommonPrefixes',
        objectPutParams: Bucket =>
            [
                { Bucket, Key: '&amp#' },
                { Bucket, Key: '"quot#' }, { Bucket, Key: '\'apos#' },
                { Bucket, Key: '<lt#' }, { Bucket, Key: '<gt#' },
            ],
        listObjectParams: Bucket => ({ Bucket, Delimiter: '#' }),
        assertions: data => {
            assert.deepStrictEqual(data.CommonPrefixes, [
                { Prefix: '"quot#' }, { Prefix: '&amp#' },
                { Prefix: '\'apos#' }, { Prefix: '<gt#' },
                { Prefix: '<lt#' }]);
        },
    },
];

describe('GET Bucket - AWS.S3.listObjects', () => {
    describe('When user is unauthorized', () => {
        let bucketUtil;
        let bucketName;

        before(done => {
            bucketUtil = new BucketUtility();
            bucketUtil.createRandom(1)
                      .then(created => {
                          bucketName = created;
                          done();
                      })
                      .catch(done);
        });

        after(done => {
            bucketUtil.deleteOne(bucketName)
                      .then(() => done())
                      .catch(done);
        });

        it('should return 403 and AccessDenied on a private bucket', done => {
            const params = { Bucket: bucketName };
            bucketUtil.s3
                .makeUnauthenticatedRequest('listObjects', params, error => {
                    assert(error);
                    assert.strictEqual(error.statusCode, 403);
                    assert.strictEqual(error.code, 'AccessDenied');
                    done();
                });
        });
    });

    withV4(sigCfg => {
        let bucketUtil;
        let bucketName;

        before(done => {
            bucketUtil = new BucketUtility('default', sigCfg);
            bucketUtil.createRandom(1)
                      .then(created => {
                          bucketName = created;
                          done();
                      })
                      .catch(done);
        });

        after(done => {
            bucketUtil.deleteOne(bucketName).then(() => done()).catch(done);
        });

        afterEach(done => {
            bucketUtil.empty(bucketName).then(() => done()).catch(done);
        });

        tests.forEach(test => {
            it(`should ${test.name}`, async () => {
                const s3 = bucketUtil.s3;
                const Bucket = bucketName;
                for (const param of test.objectPutParams(Bucket)) {
                    await s3.putObject(param).promise();
                }
                const data = await s3.listObjects(test.listObjectParams(Bucket)).promise();
                const isValidResponse = tv4.validate(data, bucketSchema);
                if (!isValidResponse) {
                    throw new Error(tv4.error);
                }
                test.assertions(data, Bucket);
            });
        });

        tests.forEach(test => {
            it(`v2 should ${test.name}`, async () => {
                const s3 = bucketUtil.s3;
                const Bucket = bucketName;

                for (const param of test.objectPutParams(Bucket)) {
                    await s3.putObject(param).promise();
                }
                const data = await s3.listObjectsV2(test.listObjectParams(Bucket)).promise();
                const isValidResponse = tv4.validate(data, bucketSchemaV2);
                if (!isValidResponse) {
                    throw new Error(tv4.error);
                }
                test.assertions(data, Bucket);
            });
        });

        ['&amp', '"quot', '\'apos', '<lt', '>gt'].forEach(k => {
            it(`should list objects with key ${k} as Prefix`, async () => {
                const s3 = bucketUtil.s3;
                const Bucket = bucketName;
                const objects = [{ Bucket, Key: k }];

                for (const param of objects) {
                    await s3.putObject(param).promise();
                }
                const data = await s3.listObjects({ Bucket, Prefix: k }).promise();
                const isValidResponse = tv4.validate(data, bucketSchema);
                if (!isValidResponse) {
                    throw new Error(tv4.error);
                }
                assert.deepStrictEqual(data.Prefix, k);
            });
        });

        ['&amp', '"quot', '\'apos', '<lt', '>gt'].forEach(k => {
            it(`should list objects with key ${k} as Marker`, async () => {
                const s3 = bucketUtil.s3;
                const Bucket = bucketName;
                const objects = [{ Bucket, Key: k }];

                for (const param of objects) {
                    await s3.putObject(param).promise();
                }
                const data = await s3.listObjects({ Bucket, Marker: k }).promise();
                const isValidResponse = tv4.validate(data, bucketSchema);
                if (!isValidResponse) {
                    throw new Error(tv4.error);
                }
                assert.deepStrictEqual(data.Marker, k);
            });
        });

        ['&amp', '"quot', '\'apos', '<lt', '>gt'].forEach(k => {
            it(`should list objects with key ${k} as NextMarker`, async () => {
                const s3 = bucketUtil.s3;
                const Bucket = bucketName;
                const objects = [{ Bucket, Key: k }, { Bucket, Key: 'zzz' }];

                for (const param of objects) {
                    await s3.putObject(param).promise();
                }
                const data = await s3.listObjects({ Bucket, MaxKeys: 1,
                    Delimiter: 'foo' }).promise();
                const isValidResponse = tv4.validate(data, bucketSchema);
                if (!isValidResponse) {
                    throw new Error(tv4.error);
                }
                assert.strictEqual(data.NextMarker, k);
            });
        });

        ['&amp', '"quot', '\'apos', '<lt', '>gt'].forEach(k => {
            it(`should list objects with key ${k} as StartAfter`, async () => {
                const s3 = bucketUtil.s3;
                const Bucket = bucketName;
                const objects = [{ Bucket, Key: k }];

                for (const param of objects) {
                    await s3.putObject(param).promise();
                }
                const data = await s3.listObjectsV2(
                    { Bucket, StartAfter: k }).promise();
                const isValidResponse = tv4.validate(data, bucketSchemaV2);
                if (!isValidResponse) {
                    throw new Error(tv4.error);
                }
                assert.deepStrictEqual(data.StartAfter, k);
            });
        });

        ['&amp', '"quot', '\'apos', '<lt', '>gt'].forEach(k => {
            it(`should list objects with key ${k} as ContinuationToken`,
            async () => {
                const s3 = bucketUtil.s3;
                const Bucket = bucketName;
                const objects = [{ Bucket, Key: k }];

                for (const param of objects) {
                    await s3.putObject(param).promise();
                }
                const data = await s3.listObjectsV2({
                    Bucket,
                    ContinuationToken: generateToken(k),
                }).promise();
                const isValidResponse = tv4.validate(data, bucketSchemaV2);
                if (!isValidResponse) {
                    throw new Error(tv4.error);
                }
                assert.deepStrictEqual(
                    decryptToken(data.ContinuationToken), k);
            });
        });

        ['&amp', '"quot', '\'apos', '<lt', '>gt'].forEach(k => {
            it(`should list objects with key ${k} as NextContinuationToken`,
            async () => {
                const s3 = bucketUtil.s3;
                const Bucket = bucketName;
                const objects = [{ Bucket, Key: k }, { Bucket, Key: 'zzz' }];

                for (const param of objects) {
                    await s3.putObject(param).promise();
                }
                const data = await s3.listObjectsV2({ Bucket, MaxKeys: 1,
                    Delimiter: 'foo' }).promise();
                const isValidResponse = tv4.validate(data, bucketSchemaV2);
                if (!isValidResponse) {
                    throw new Error(tv4.error);
                }
                assert.strictEqual(
                    decryptToken(data.NextContinuationToken), k);
            });
        });

        const describeBypass = isVaultScality && internalPortBypassBP ? describe : describe.skip;
        describeBypass('x-amz-optional-attributes header', () => {
            let policyWithoutPermission;
            let userWithoutPermission;
            let s3ClientWithoutPermission;

            const iamConfig = getConfig('default', { region: 'us-east-1' });
            iamConfig.endpoint = `http://${vaultHost}:8600`;
            const iamClient = new IAM(iamConfig);

            before(async () => {
                const policyRes = await iamClient
                    .createPolicy({
                        PolicyName: 'bp-bypass-policy',
                        PolicyDocument: JSON.stringify({
                            Version: '2012-10-17',
                            Statement: [{
                                Sid: 'AllowS3ListBucket',
                                Effect: 'Allow',
                                Action: [
                                    's3:ListBucket',
                                ],
                                Resource: ['*'],
                            }],
                        }),
                    })
                    .promise();
                policyWithoutPermission = policyRes.Policy;
                const userRes = await iamClient.createUser({ UserName: 'user-without-permission' }).promise();
                userWithoutPermission = userRes.User;
                await iamClient
                    .attachUserPolicy({
                        UserName: userWithoutPermission.UserName,
                        PolicyArn: policyWithoutPermission.Arn,
                    })
                    .promise();

                const accessKeyRes = await iamClient.createAccessKey({
                    UserName: userWithoutPermission.UserName,
                }).promise();
                const accessKey = accessKeyRes.AccessKey;
                const s3Config = getConfig('default', {
                    credentials: new AWS.Credentials(accessKey.AccessKeyId, accessKey.SecretAccessKey),
                });
                s3ClientWithoutPermission = new AWS.S3(s3Config);
            });

            after(async () => {
                await iamClient
                    .detachUserPolicy({
                        UserName: userWithoutPermission.UserName,
                        PolicyArn: policyWithoutPermission.Arn,
                    })
                    .promise();
                await iamClient.deletePolicy({ PolicyArn: policyWithoutPermission.Arn }).promise();
                await iamClient.deleteUser({ UserName: userWithoutPermission.UserName }).promise();
            });

            // eslint-disable-next-line max-len
            const listObjectsV2WithOptionalAttributes = async (s3, bucket, headerValue) => await new Promise((resolve, reject) => {
                let rawXml = '';
                const req = s3.listObjectsV2({ Bucket: bucket });

                req.on('build', () => {
                    req.httpRequest.headers['x-amz-optional-object-attributes'] = headerValue;
                });
                req.on('httpData', chunk => { rawXml += chunk; });
                req.on('error', err => reject(err));
                req.on('success', response => {
                    parseString(rawXml, (err, parsedXml) => {
                        if (err) {
                            return reject(err);
                        }

                        const contents = response.data.Contents;
                        const parsedContents = parsedXml.ListBucketResult.Contents;

                        if (!contents || !parsedContents) {
                            return resolve(response.data);
                        }

                        if (parsedContents[0]?.['x-amz-meta-department']) {
                            contents[0]['x-amz-meta-department'] = parsedContents[0]['x-amz-meta-department'][0];
                        }

                        if (parsedContents[0]?.['x-amz-meta-hr']) {
                            contents[0]['x-amz-meta-hr'] = parsedContents[0]['x-amz-meta-hr'][0];
                        }

                        return resolve(response.data);
                    });
                });

                req.send();
            });
            
            it('should return an XML if the header is set', async () => {
                const s3 = bucketUtil.s3;
                const Bucket = bucketName;

                await s3.putObject({
                    Bucket,
                    Key: 'super-power-object',
                    Metadata: {
                        Department: 'sales',
                        HR: 'true',
                    },
                }).promise();
                const result = await listObjectsV2WithOptionalAttributes(
                    s3,
                    Bucket,
                    'x-amz-meta-*,RestoreStatus,x-amz-meta-department',
                );

                assert.strictEqual(result.Contents.length, 1);
                assert.strictEqual(result.Contents[0].Key, 'super-power-object');
                assert.strictEqual(result.Contents[0]['x-amz-meta-department'], 'sales');
                assert.strictEqual(result.Contents[0]['x-amz-meta-hr'], 'true');
            });

            it('should reject the request if the user does not have the permission', async () => {
                const s3 = bucketUtil.s3;
                const Bucket = bucketName;

                await s3.putObject({
                    Bucket,
                    Key: 'super-power-object',
                    Metadata: {
                        Department: 'sales',
                        HR: 'true',
                    },
                }).promise();

                try {
                    await listObjectsV2WithOptionalAttributes(
                        s3ClientWithoutPermission,
                        Bucket,
                        'x-amz-meta-*,RestoreStatus,x-amz-meta-department',
                    );
                    throw new Error('Request should have been rejected');
                } catch (err) {
                    assert.strictEqual(err.statusCode, 403);
                    assert.strictEqual(err.code, 'AccessDenied');
                }
            });

            it('should return an XML if the header is only RestoreStatus even without permission', async () => {
                const s3 = bucketUtil.s3;
                const Bucket = bucketName;

                await s3.putObject({
                    Bucket,
                    Key: 'super-power-object',
                    Metadata: {
                        Department: 'sales',
                        HR: 'true',
                    },
                }).promise();
                const result = await listObjectsV2WithOptionalAttributes(
                    s3ClientWithoutPermission,
                    Bucket,
                    'RestoreStatus',
                );

                assert.strictEqual(result.Contents.length, 1);
                assert.strictEqual(result.Contents[0].Key, 'super-power-object');
                assert.strictEqual(result.Contents[0]['x-amz-meta-department'], undefined);
                assert.strictEqual(result.Contents[0]['x-amz-meta-hr'], undefined);
            });
        });
    });
});
