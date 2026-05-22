const assert = require('assert');
const tv4 = require('tv4');
const {
    PutObjectCommand,
    ListObjectsCommand,
    ListObjectsV2Command,
    PutBucketPolicyCommand,
    DeleteBucketPolicyCommand,
    S3Client,
} = require('@aws-sdk/client-s3');
const { ListObjectsV2ExtendedCommand } = require('@scality/cloudserverclient');
const {
    IAMClient,
    CreatePolicyCommand,
    CreateUserCommand,
    AttachUserPolicyCommand,
    CreateAccessKeyCommand,
    DetachUserPolicyCommand,
    DeletePolicyCommand,
    DeleteUserCommand,
} = require('@aws-sdk/client-iam');
const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');
const bucketSchema = require('../../schema/bucket');
const bucketSchemaV2 = require('../../schema/bucketV2');
const { generateToken, decryptToken } = require('../../../../../lib/api/apiUtils/object/continueToken');
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
        let bucketName;
        let authenticatedBucketUtil;
        let unauthenticatedBucketUtil;

        before(done => {
            authenticatedBucketUtil = new BucketUtility('default', {});
            unauthenticatedBucketUtil = new BucketUtility('default', {}, true);
            authenticatedBucketUtil.createRandom(1)
                      .then(created => {
                          bucketName = created;
                          done();
                      })
                      .catch(done);
        });

        after(done => {
            authenticatedBucketUtil.deleteOne(bucketName)
                      .then(() => done())
                      .catch(done);
        });

        it('should return 403 and AccessDenied on a private bucket', done => {
            const params = { Bucket: bucketName };
            unauthenticatedBucketUtil.s3.send(new ListObjectsCommand(params))
                .then(() => {
                    assert.fail('Expected request to fail with AccessDenied');
                })
                .catch(error => {
                    assert.strictEqual(error.$metadata.httpStatusCode, 403);
                    assert.strictEqual(error.name, 'AccessDenied');
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

        after(() => bucketUtil.deleteOne(bucketName));

        afterEach(() => bucketUtil.empty(bucketName));

        tests.forEach(test => {
            it(`should ${test.name}`, async () => {
                const s3 = bucketUtil.s3;
                const Bucket = bucketName;
                for (const param of test.objectPutParams(Bucket)) {
                    await s3.send(new PutObjectCommand(param));
                }
                const { $metadata, ...data } = await s3.send(new ListObjectsCommand(test.listObjectParams(Bucket)));
                const validationSchema = {
                    ...bucketSchema,
                    required: bucketSchema.required.filter(field => Object.prototype.hasOwnProperty.call(data, field))
                };
                const isValidResponse = tv4.validate(data, validationSchema);
                if (!isValidResponse) {
                    throw new Error(tv4.error);
                }
                test.assertions(data, Bucket);
                assert.strictEqual($metadata.httpStatusCode, 200);
            });
        });

        tests.forEach(test => {
            it(`v2 should ${test.name}`, async () => {
                const s3 = bucketUtil.s3;
                const Bucket = bucketName;

                for (const param of test.objectPutParams(Bucket)) {
                    await s3.send(new PutObjectCommand(param));
                }
                const { $metadata, ...data } = await s3.send(new ListObjectsV2Command(test.listObjectParams(Bucket)));
                const validationSchema2 = {
                    ...bucketSchemaV2,
                    required: bucketSchemaV2.required.filter(field => Object.prototype.hasOwnProperty.call(data, field))
                };
                const isValidResponse = tv4.validate(data, validationSchema2);
                if (!isValidResponse) {
                    throw new Error(tv4.error);
                }
                test.assertions(data, Bucket);
                assert.strictEqual($metadata.httpStatusCode, 200);
            });
        });

        ['&amp', '"quot', '\'apos', '<lt', '>gt'].forEach(k => {
            it(`should list objects with key ${k} as Prefix`, async () => {
                const s3 = bucketUtil.s3;
                const Bucket = bucketName;
                const objects = [{ Bucket, Key: k }];

                for (const param of objects) {
                                await s3.send(new PutObjectCommand(param));
        }
                const { $metadata, ...data } = await s3.send(new ListObjectsCommand({ Bucket, Prefix: k }));
                const validationSchema = {
                    ...bucketSchema,
                    required: bucketSchema.required.filter(field => Object.prototype.hasOwnProperty.call(data, field))
                };
                const isValidResponse = tv4.validate(data, validationSchema);
                if (!isValidResponse) {
                    throw new Error(tv4.error);
                }
                assert.deepStrictEqual(data.Prefix, k);
                assert.strictEqual($metadata.httpStatusCode, 200);
            });
        });

        ['&amp', '"quot', '\'apos', '<lt', '>gt'].forEach(k => {
            it(`should list objects with key ${k} as Marker`, async () => {
                const s3 = bucketUtil.s3;
                const Bucket = bucketName;
                const objects = [{ Bucket, Key: k }];

                for (const param of objects) {
                    await s3.send(new PutObjectCommand(param));
                }
                const { $metadata, ...data } = await s3.send(new ListObjectsCommand({ Bucket, Marker: k }));
                const validationSchema = {
                    ...bucketSchema,
                    required: bucketSchema.required.filter(field => Object.prototype.hasOwnProperty.call(data, field))
                };
                const isValidResponse = tv4.validate(data, validationSchema);
                if (!isValidResponse) {
                    throw new Error(tv4.error);
                }
                assert.deepStrictEqual(data.Marker, k);
                assert.strictEqual($metadata.httpStatusCode, 200);
            });
        });

        ['&amp', '"quot', '\'apos', '<lt', '>gt'].forEach(k => {
            it(`should list objects with key ${k} as NextMarker`, async () => {
                const s3 = bucketUtil.s3;
                const Bucket = bucketName;
                const objects = [{ Bucket, Key: k }, { Bucket, Key: 'zzz' }];

                for (const param of objects) {
                    await s3.send(new PutObjectCommand(param));
                }
                const { $metadata, ...data } = await s3.send(new ListObjectsCommand({ Bucket, MaxKeys: 1,
                    Delimiter: 'foo' }));

                const validationSchema = {
                    ...bucketSchema,
                    required: bucketSchema.required.filter(field => Object.prototype.hasOwnProperty.call(data, field))
                };
                const isValidResponse = tv4.validate(data, validationSchema);
                if (!isValidResponse) {
                    throw new Error(tv4.error);
                }
                assert.strictEqual(data.NextMarker, k);
                assert.strictEqual($metadata.httpStatusCode, 200);
            });
        });

        ['&amp', '"quot', '\'apos', '<lt', '>gt'].forEach(k => {
            it(`should list objects with key ${k} as StartAfter`, async () => {
                const s3 = bucketUtil.s3;
                const Bucket = bucketName;
                const objects = [{ Bucket, Key: k }];

                for (const param of objects) {
                    await s3.send(new PutObjectCommand(param));
                }
                const { $metadata, ...data } = await s3.send(new ListObjectsV2Command({ Bucket, StartAfter: k }));
                const validationSchema2 = {
                    ...bucketSchemaV2,
                    required: bucketSchemaV2.required.filter(field => Object.prototype.hasOwnProperty.call(data, field))
                };
                const isValidResponse = tv4.validate(data, validationSchema2);
                if (!isValidResponse) {
                    throw new Error(tv4.error);
                }
                assert.deepStrictEqual(data.StartAfter, k);
                assert.strictEqual($metadata.httpStatusCode, 200);
            });
        });

        ['&amp', '"quot', '\'apos', '<lt', '>gt'].forEach(k => {
            it(`should list objects with key ${k} as ContinuationToken`,
            async () => {
                const s3 = bucketUtil.s3;
                const Bucket = bucketName;
                const objects = [{ Bucket, Key: k }];

                for (const param of objects) {
                    await s3.send(new PutObjectCommand(param));
                }
                const { $metadata, ...data } = await s3.send(new ListObjectsV2Command({
                    Bucket,
                    ContinuationToken: generateToken(k),
                }));
                const validationSchema2 = {
                    ...bucketSchemaV2,
                    required: bucketSchemaV2.required.filter(field => Object.prototype.hasOwnProperty.call(data, field))
                };
                const isValidResponse = tv4.validate(data, validationSchema2);
                if (!isValidResponse) {
                    throw new Error(tv4.error);
                }
                assert.deepStrictEqual(
                    decryptToken(data.ContinuationToken), k);
                assert.strictEqual($metadata.httpStatusCode, 200);
            });
        });

        ['&amp', '"quot', '\'apos', '<lt', '>gt'].forEach(k => {
            it(`should list objects with key ${k} as NextContinuationToken`,
            async () => {
                const s3 = bucketUtil.s3;
                const Bucket = bucketName;
                const objects = [{ Bucket, Key: k }, { Bucket, Key: 'zzz' }];

                for (const param of objects) {
                    await s3.send(new PutObjectCommand(param));
                }
                const { $metadata, ...data } = await s3.send(new ListObjectsV2Command({ Bucket, MaxKeys: 1,
                    Delimiter: 'foo' }));
                const validationSchema2 = {
                    ...bucketSchemaV2,
                    required: bucketSchemaV2.required.filter(field => Object.prototype.hasOwnProperty.call(data, field))
                };
                const isValidResponse = tv4.validate(data, validationSchema2);
                if (!isValidResponse) {
                    throw new Error(tv4.error);
                }
                assert.strictEqual(
                    decryptToken(data.NextContinuationToken), k);
                assert.strictEqual($metadata.httpStatusCode, 200);
            });
        });

        const describeBypass = isVaultScality && internalPortBypassBP ? describe : describe.skip;
        describeBypass('x-amz-optional-attributes header', () => {
            let policyWithListBucketOnly;
            let userWithListBucketOnly;
            let s3ClientWithListBucketOnly;

            const iamConfig = getConfig('default', { region: 'us-east-1' });
            iamConfig.endpoint = `http://${vaultHost}:8600`;
            const iamClient = new IAMClient(iamConfig);

            before(async () => {
                const policyRes = await iamClient.send(new CreatePolicyCommand({
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
                }));
                policyWithListBucketOnly = policyRes.Policy;
                const userRes = await iamClient.send(new CreateUserCommand({ UserName: 'user-without-permission' }));
                userWithListBucketOnly = userRes.User;
                await iamClient.send(new AttachUserPolicyCommand({
                    UserName: userWithListBucketOnly.UserName,
                    PolicyArn: policyWithListBucketOnly.Arn,
                }));

                const accessKeyRes = await iamClient.send(new CreateAccessKeyCommand({
                    UserName: userWithListBucketOnly.UserName,
                }));
                const accessKey = accessKeyRes.AccessKey;
                const s3Config = getConfig('default', {
                    credentials: {
                        accessKeyId: accessKey.AccessKeyId,
                        secretAccessKey: accessKey.SecretAccessKey,
                    },
                });
                s3ClientWithListBucketOnly = new S3Client(s3Config);
            });

            after(async () => {
                await iamClient.send(new DetachUserPolicyCommand({
                    UserName: userWithListBucketOnly.UserName,
                    PolicyArn: policyWithListBucketOnly.Arn,
                }));
                await iamClient.send(new DeletePolicyCommand({ PolicyArn: policyWithListBucketOnly.Arn }));
                await iamClient.send(new DeleteUserCommand({ UserName: userWithListBucketOnly.UserName }));
            });

            it('should return an XML if the header is set', async () => {
                const s3 = bucketUtil.s3;
                const Bucket = bucketName;

                await s3.send(new PutObjectCommand({
                    Bucket,
                    Key: 'super-power-object',
                    Metadata: {
                        department: 'sales',
                        hr: 'true',
                    },
                }));
                const result = await s3.send(new ListObjectsV2ExtendedCommand({
                    Bucket,
                    ObjectAttributes: ['x-amz-meta-*', 'RestoreStatus', 'x-amz-meta-department'],
                }));

                assert.strictEqual(result.Contents.length, 1);
                assert.strictEqual(result.Contents[0].Key, 'super-power-object');
                assert.strictEqual(result.Contents[0]['x-amz-meta-department'], 'sales');
                assert.strictEqual(result.Contents[0]['x-amz-meta-hr'], 'true');
            });

            it('should reject the request if the user does not have the permission', async () => {
                const s3 = bucketUtil.s3;
                const Bucket = bucketName;

                await s3.send(new PutObjectCommand({
                    Bucket,
                    Key: 'super-power-object',
                    Metadata: {
                        department: 'sales',
                        hr: 'true',
                    },
                }));

                try {
                    await s3ClientWithListBucketOnly.send(new ListObjectsV2ExtendedCommand({
                        Bucket,
                        ObjectAttributes: ['x-amz-meta-*', 'RestoreStatus', 'x-amz-meta-department'],
                    }));
                    throw new Error('Request should have been rejected');
                } catch (err) {
                    if (err.message === 'Request should have been rejected') {
                        throw err;
                    }
                    assert.strictEqual(err.$metadata.httpStatusCode, 403);
                    assert.strictEqual(err.name, 'AccessDenied');
                }
            });

            it('should always (ignore permission) return an XML when the header is RestoreStatus', async () => {
                const s3 = bucketUtil.s3;
                const Bucket = bucketName;

                await s3.send(new PutObjectCommand({
                    Bucket,
                    Key: 'super-power-object',
                    Metadata: {
                        department: 'sales',
                        hr: 'true',
                    },
                }));
                const result = await s3ClientWithListBucketOnly.send(new ListObjectsV2ExtendedCommand({
                    Bucket,
                    ObjectAttributes: ['RestoreStatus'],
                }));

                assert.strictEqual(result.Contents.length, 1);
                assert.strictEqual(result.Contents[0].Key, 'super-power-object');
                assert.strictEqual(result.Contents[0]['x-amz-meta-department'], undefined);
                assert.strictEqual(result.Contents[0]['x-amz-meta-hr'], undefined);
            });

            describe('AccessDenied when scality:ListBucketOptionalObjectAttributes is not granted', () => {
                let userOptAttrsOnly;
                let policyOptAttrsOnly;
                let s3ClientOptAttrsOnly;

                let userNoPermissions;
                let s3ClientNoPermissions;

                let userListAllowAttrsDeny;
                let policyListAllowAttrsDeny;
                let s3ClientListAllowAttrsDeny;

                const setupIamUser = async (userName, policyDoc) => {
                    let policy;
                    if (policyDoc) {
                        const res = await iamClient.send(new CreatePolicyCommand({
                            PolicyName: `${userName}-policy`,
                            PolicyDocument: JSON.stringify(policyDoc),
                        }));
                        policy = res.Policy;
                    }
                    const userRes = await iamClient.send(new CreateUserCommand({ UserName: userName }));
                    if (policy) {
                        await iamClient.send(new AttachUserPolicyCommand({
                            UserName: userName,
                            PolicyArn: policy.Arn,
                        }));
                    }
                    const accessKeyRes = await iamClient.send(new CreateAccessKeyCommand({
                        UserName: userName,
                    }));
                    const ak = accessKeyRes.AccessKey;
                    const s3Cfg = getConfig('default', {
                        credentials: {
                            accessKeyId: ak.AccessKeyId,
                            secretAccessKey: ak.SecretAccessKey,
                        },
                    });
                    return { user: userRes.User, policy, s3: new S3Client(s3Cfg) };
                };

                const teardownIamUser = async (user, policy) => {
                    if (policy) {
                        await iamClient.send(new DetachUserPolicyCommand({
                            UserName: user.UserName,
                            PolicyArn: policy.Arn,
                        }));
                        await iamClient.send(new DeletePolicyCommand({ PolicyArn: policy.Arn }));
                    }
                    await iamClient.send(new DeleteUserCommand({ UserName: user.UserName }));
                };

                before(async () => {
                    ({
                        user: userOptAttrsOnly,
                        policy: policyOptAttrsOnly,
                        s3: s3ClientOptAttrsOnly,
                    } = await setupIamUser('user-opt-attrs-only', {
                        Version: '2012-10-17',
                        Statement: [{
                            Sid: 'AllowOptAttrsOnly',
                            Effect: 'Allow',
                            Action: ['scality:ListBucketOptionalObjectAttributes'],
                            Resource: ['*'],
                        }],
                    }));

                    ({
                        user: userNoPermissions,
                        s3: s3ClientNoPermissions,
                    } = await setupIamUser('user-no-permissions', null));

                    ({
                        user: userListAllowAttrsDeny,
                        policy: policyListAllowAttrsDeny,
                        s3: s3ClientListAllowAttrsDeny,
                    } = await setupIamUser('user-list-allow-attrs-deny', {
                        Version: '2012-10-17',
                        Statement: [
                            {
                                Sid: 'AllowS3ListBucket',
                                Effect: 'Allow',
                                Action: ['s3:ListBucket'],
                                Resource: ['*'],
                            },
                            {
                                Sid: 'DenyOptAttrs',
                                Effect: 'Deny',
                                Action: ['scality:ListBucketOptionalObjectAttributes'],
                                Resource: ['*'],
                            },
                        ],
                    }));
                });

                after(async () => {
                    await teardownIamUser(userOptAttrsOnly, policyOptAttrsOnly);
                    await teardownIamUser(userNoPermissions, null);
                    await teardownIamUser(userListAllowAttrsDeny, policyListAllowAttrsDeny);
                });

                it('should reject when user has only the new permission and not s3:ListBucket', async () => {
                    try {
                        await s3ClientOptAttrsOnly.send(new ListObjectsV2ExtendedCommand({
                            Bucket: bucketName,
                            ObjectAttributes: ['x-amz-meta-foo'],
                        }));
                        throw new Error('Request should have been rejected');
                    } catch (err) {
                        if (err.message === 'Request should have been rejected') {
                            throw err;
                        }
                        assert.strictEqual(err.$metadata.httpStatusCode, 403);
                        assert.strictEqual(err.name, 'AccessDenied');
                    }
                });

                it('should reject when user has neither permission', async () => {
                    try {
                        await s3ClientNoPermissions.send(new ListObjectsV2ExtendedCommand({
                            Bucket: bucketName,
                            ObjectAttributes: ['x-amz-meta-foo'],
                        }));
                        throw new Error('Request should have been rejected');
                    } catch (err) {
                        if (err.message === 'Request should have been rejected') {
                            throw err;
                        }
                        assert.strictEqual(err.$metadata.httpStatusCode, 403);
                        assert.strictEqual(err.name, 'AccessDenied');
                    }
                });

                it('should reject when explicit deny on the new permission overrides allow', async () => {
                    try {
                        await s3ClientListAllowAttrsDeny.send(new ListObjectsV2ExtendedCommand({
                            Bucket: bucketName,
                            ObjectAttributes: ['x-amz-meta-foo'],
                        }));
                        throw new Error('Request should have been rejected');
                    } catch (err) {
                        if (err.message === 'Request should have been rejected') {
                            throw err;
                        }
                        assert.strictEqual(err.$metadata.httpStatusCode, 403);
                        assert.strictEqual(err.name, 'AccessDenied');
                    }
                });
            });

            describe('bucket policy evaluation of scality:ListBucketOptionalObjectAttributes', () => {
                let userWithBothPerms;
                let policyWithBothPerms;
                let s3ClientWithBothPerms;

                before(async () => {
                    const userName = 'user-with-both-perms';
                    const policyRes = await iamClient.send(new CreatePolicyCommand({
                        PolicyName: `${userName}-policy`,
                        PolicyDocument: JSON.stringify({
                            Version: '2012-10-17',
                            Statement: [
                                {
                                    Effect: 'Allow',
                                    Action: ['s3:ListBucket'],
                                    Resource: ['*'],
                                },
                                {
                                    Effect: 'Allow',
                                    Action: ['scality:ListBucketOptionalObjectAttributes'],
                                    Resource: ['*'],
                                },
                            ],
                        }),
                    }));
                    policyWithBothPerms = policyRes.Policy;
                    const userRes = await iamClient.send(new CreateUserCommand({ UserName: userName }));
                    userWithBothPerms = userRes.User;
                    await iamClient.send(new AttachUserPolicyCommand({
                        UserName: userName,
                        PolicyArn: policyWithBothPerms.Arn,
                    }));
                    const accessKeyRes = await iamClient.send(new CreateAccessKeyCommand({
                        UserName: userName,
                    }));
                    const ak = accessKeyRes.AccessKey;
                    const s3Cfg = getConfig('default', {
                        credentials: {
                            accessKeyId: ak.AccessKeyId,
                            secretAccessKey: ak.SecretAccessKey,
                        },
                    });
                    s3ClientWithBothPerms = new S3Client(s3Cfg);
                });

                after(async () => {
                    await iamClient.send(new DetachUserPolicyCommand({
                        UserName: userWithBothPerms.UserName,
                        PolicyArn: policyWithBothPerms.Arn,
                    }));
                    await iamClient.send(new DeletePolicyCommand({ PolicyArn: policyWithBothPerms.Arn }));
                    await iamClient.send(new DeleteUserCommand({ UserName: userWithBothPerms.UserName }));
                });

                afterEach(async () => {
                    await bucketUtil.s3
                        .send(new DeleteBucketPolicyCommand({ Bucket: bucketName }))
                        .catch(() => {});
                });

                // eslint-disable-next-line max-len
                it('should allow when the bucket policy supplies scality:ListBucketOptionalObjectAttributes that IAM lacks', async () => {
                    await bucketUtil.s3.send(new PutBucketPolicyCommand({
                        Bucket: bucketName,
                        Policy: JSON.stringify({
                            Version: '2012-10-17',
                            Statement: [{
                                Effect: 'Allow',
                                Principal: { AWS: userWithListBucketOnly.Arn },
                                Action: ['scality:ListBucketOptionalObjectAttributes'],
                                Resource: [
                                    `arn:aws:s3:::${bucketName}`,
                                    `arn:aws:s3:::${bucketName}/*`,
                                ],
                            }],
                        }),
                    }));

                    await bucketUtil.s3.send(new PutObjectCommand({
                        Bucket: bucketName,
                        Key: 'object-with-color',
                        Metadata: { color: 'red' },
                    }));

                    const result = await s3ClientWithListBucketOnly.send(new ListObjectsV2ExtendedCommand({
                        Bucket: bucketName,
                        ObjectAttributes: ['x-amz-meta-color'],
                    }));

                    assert.ok(Array.isArray(result.Contents));
                    assert.strictEqual(result.Contents.length, 1);
                    assert.strictEqual(result.Contents[0].Key, 'object-with-color');
                });

                it('should reject when the bucket policy denies the new action even if IAM allows it', async () => {
                    await bucketUtil.s3.send(new PutBucketPolicyCommand({
                        Bucket: bucketName,
                        Policy: JSON.stringify({
                            Version: '2012-10-17',
                            Statement: [{
                                Effect: 'Deny',
                                Principal: { AWS: userWithBothPerms.Arn },
                                Action: ['scality:ListBucketOptionalObjectAttributes'],
                                Resource: [
                                    `arn:aws:s3:::${bucketName}`,
                                    `arn:aws:s3:::${bucketName}/*`,
                                ],
                            }],
                        }),
                    }));

                    try {
                        await s3ClientWithBothPerms.send(new ListObjectsV2ExtendedCommand({
                            Bucket: bucketName,
                            ObjectAttributes: ['x-amz-meta-foo'],
                        }));
                        throw new Error('Request should have been rejected');
                    } catch (err) {
                        if (err.message === 'Request should have been rejected') {
                            throw err;
                        }
                        assert.strictEqual(err.$metadata.httpStatusCode, 403);
                        assert.strictEqual(err.name, 'AccessDenied');
                    }
                });
            });
        });
    });
});
