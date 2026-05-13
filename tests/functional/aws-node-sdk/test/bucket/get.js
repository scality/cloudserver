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

        const listObjectsV2WithOptionalAttributes = (
            s3, Bucket, headerValue, extraParams = {},
        ) => new Promise((resolve, reject) => {
            let rawXml = '';
            const req = s3.listObjectsV2({ Bucket, ...extraParams });
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
                    return resolve({ data: response.data, parsedXml, rawXml });
                });
            });
            req.send();
        });

        describe('x-amz-optional-object-attributes', () => {
            it('should reject a malformed header value with 400 InvalidArgument', async () => {
                const s3 = bucketUtil.s3;
                try {
                    await listObjectsV2WithOptionalAttributes(s3, bucketName, 'Foo');
                    throw new Error('Request should have been rejected');
                } catch (err) {
                    assert.strictEqual(err.statusCode, 400);
                    assert.strictEqual(err.code, 'InvalidArgument');
                }
            });

            it('should return the requested user-metadata element in the listing response', async () => {
                const s3 = bucketUtil.s3;
                await s3.putObject({
                    Bucket: bucketName,
                    Key: 'object-with-color',
                    Metadata: { color: 'red' },
                }).promise();

                const { data, parsedXml } = await listObjectsV2WithOptionalAttributes(
                    s3,
                    bucketName,
                    'x-amz-meta-color',
                );

                assert.ok(Array.isArray(data.Contents));
                assert.strictEqual(data.Contents.length, 1);

                const contents = parsedXml.ListBucketResult.Contents;
                assert.strictEqual(contents[0].Key[0], 'object-with-color');
                assert.deepStrictEqual(contents[0]['x-amz-meta-color'], ['red']);
            });

            it('should omit the user-metadata element when the object has no matching metadata', async () => {
                const s3 = bucketUtil.s3;
                await s3.putObject({
                    Bucket: bucketName,
                    Key: 'object-without-metadata',
                }).promise();

                const { parsedXml } = await listObjectsV2WithOptionalAttributes(
                    s3,
                    bucketName,
                    'x-amz-meta-color',
                );

                const contents = parsedXml.ListBucketResult.Contents;
                assert.strictEqual(contents.length, 1);
                assert.strictEqual(contents[0].Key[0], 'object-without-metadata');
                assert.strictEqual(contents[0]['x-amz-meta-color'], undefined);
            });

            it('should use HEAD\'s key case for the user-metadata element regardless of header case', async () => {
                const s3 = bucketUtil.s3;
                await s3.putObject({
                    Bucket: bucketName,
                    Key: 'case-probe-object',
                    Metadata: { MyKey: 'foo' },
                }).promise();

                const head = await s3.headObject({
                    Bucket: bucketName,
                    Key: 'case-probe-object',
                }).promise();

                const metadataKeys = Object.keys(head.Metadata || {});
                assert.strictEqual(metadataKeys.length, 1);
                const storedKeyCase = metadataKeys[0];
                assert.strictEqual(head.Metadata[storedKeyCase], 'foo');
                const expectedElementName = `x-amz-meta-${storedKeyCase}`;

                for (const headerCase of ['x-amz-meta-mykey', 'x-amz-meta-MyKey']) {
                    const { parsedXml } = await listObjectsV2WithOptionalAttributes(
                        s3,
                        bucketName,
                        headerCase,
                    );
                    const contents = parsedXml.ListBucketResult.Contents;
                    assert.strictEqual(contents.length, 1);
                    assert.strictEqual(contents[0].Key[0], 'case-probe-object');
                    assert.deepStrictEqual(
                        contents[0][expectedElementName],
                        ['foo'],
                        `header "${headerCase}" should return element "${expectedElementName}"`,
                    );
                }
            });

            it('should return all user-metadata elements when the header is the wildcard', async () => {
                const s3 = bucketUtil.s3;
                await s3.putObject({
                    Bucket: bucketName,
                    Key: 'wildcard-object',
                    Metadata: { color: 'red', size: 'large' },
                }).promise();

                const { parsedXml } = await listObjectsV2WithOptionalAttributes(
                    s3,
                    bucketName,
                    'x-amz-meta-*',
                );

                const contents = parsedXml.ListBucketResult.Contents;
                assert.strictEqual(contents.length, 1);
                assert.strictEqual(contents[0].Key[0], 'wildcard-object');
                assert.deepStrictEqual(contents[0]['x-amz-meta-color'], ['red']);
                assert.deepStrictEqual(contents[0]['x-amz-meta-size'], ['large']);
            });

            it('should omit the user-metadata element for objects with no metadata', async () => {
                const s3 = bucketUtil.s3;
                await s3.putObject({
                    Bucket: bucketName,
                    Key: 'a-object-with-meta',
                    Metadata: { color: 'red' },
                }).promise();
                await s3.putObject({
                    Bucket: bucketName,
                    Key: 'b-object-without-meta',
                }).promise();

                const { parsedXml } = await listObjectsV2WithOptionalAttributes(
                    s3,
                    bucketName,
                    'x-amz-meta-*',
                );

                const contents = parsedXml.ListBucketResult.Contents;
                assert.strictEqual(contents.length, 2);
                const byKey = Object.fromEntries(contents.map(c => [c.Key[0], c]));
                assert.deepStrictEqual(byKey['a-object-with-meta']['x-amz-meta-color'], ['red']);
                assert.strictEqual(byKey['b-object-without-meta']['x-amz-meta-color'], undefined);
            });

            it('should not duplicate elements when wildcard and explicit key are both requested', async () => {
                const s3 = bucketUtil.s3;
                await s3.putObject({
                    Bucket: bucketName,
                    Key: 'no-dup-object',
                    Metadata: { color: 'red', size: 'large' },
                }).promise();

                const { parsedXml } = await listObjectsV2WithOptionalAttributes(
                    s3,
                    bucketName,
                    'x-amz-meta-*,x-amz-meta-color',
                );

                const contents = parsedXml.ListBucketResult.Contents;
                assert.strictEqual(contents.length, 1);
                assert.strictEqual(contents[0].Key[0], 'no-dup-object');
                assert.deepStrictEqual(contents[0]['x-amz-meta-color'], ['red']);
                assert.deepStrictEqual(contents[0]['x-amz-meta-size'], ['large']);
            });

            it('should return IsRestoreInProgress=false and no RestoreExpiryDate for non-restored object', async () => {
                const s3 = bucketUtil.s3;
                await s3.putObject({
                    Bucket: bucketName,
                    Key: 'restore-status-object',
                }).promise();

                const { parsedXml } = await listObjectsV2WithOptionalAttributes(
                    s3,
                    bucketName,
                    'RestoreStatus',
                );

                const contents = parsedXml.ListBucketResult.Contents;
                assert.strictEqual(contents.length, 1);
                assert.strictEqual(contents[0].Key[0], 'restore-status-object');

                const restoreStatus = contents[0].RestoreStatus;
                assert.ok(Array.isArray(restoreStatus));
                assert.strictEqual(restoreStatus.length, 1);
                assert.deepStrictEqual(restoreStatus[0].IsRestoreInProgress, ['false']);
                assert.strictEqual(restoreStatus[0].RestoreExpiryDate, undefined);
            });

            it('should return each version\'s own user-metadata in ListObjectVersions', async () => {
                const s3 = bucketUtil.s3;
                await s3.putBucketVersioning({
                    Bucket: bucketName,
                    VersioningConfiguration: { Status: 'Enabled' },
                }).promise();

                await s3.putObject({
                    Bucket: bucketName,
                    Key: 'versioned-object',
                    Metadata: { color: 'red' },
                }).promise();
                await s3.putObject({
                    Bucket: bucketName,
                    Key: 'versioned-object',
                    Metadata: { color: 'blue' },
                }).promise();

                const parsedXml = await new Promise((resolve, reject) => {
                    let rawXml = '';
                    const req = s3.listObjectVersions({ Bucket: bucketName });
                    req.on('build', () => {
                        req.httpRequest.headers['x-amz-optional-object-attributes'] = 'x-amz-meta-color';
                    });
                    req.on('httpData', chunk => { rawXml += chunk; });
                    req.on('error', err => reject(err));
                    req.on('success', () => {
                        parseString(rawXml, (err, parsed) => {
                            if (err) {
                                return reject(err);
                            }
                            return resolve(parsed);
                        });
                    });
                    req.send();
                });

                const versions = parsedXml.ListVersionsResult.Version;
                assert.strictEqual(versions.length, 2);
                for (const v of versions) {
                    assert.strictEqual(v.Key[0], 'versioned-object');
                    assert.ok(Array.isArray(v['x-amz-meta-color']));
                    assert.strictEqual(v['x-amz-meta-color'].length, 1);
                }
                const colors = versions.map(v => v['x-amz-meta-color'][0]).sort();
                assert.deepStrictEqual(colors, ['blue', 'red']);

                const latest = versions.find(v => v.IsLatest[0] === 'true');
                assert.strictEqual(latest['x-amz-meta-color'][0], 'blue');
            });

            it('should not include optional-attributes on delete markers in ListObjectVersions', async () => {
                const s3 = bucketUtil.s3;
                await s3.putBucketVersioning({
                    Bucket: bucketName,
                    VersioningConfiguration: { Status: 'Enabled' },
                }).promise();

                await s3.putObject({
                    Bucket: bucketName,
                    Key: 'delete-marker-object',
                    Metadata: { color: 'red' },
                }).promise();
                await s3.deleteObject({
                    Bucket: bucketName,
                    Key: 'delete-marker-object',
                }).promise();

                const parsedXml = await new Promise((resolve, reject) => {
                    let rawXml = '';
                    const req = s3.listObjectVersions({ Bucket: bucketName });
                    req.on('build', () => {
                        req.httpRequest.headers['x-amz-optional-object-attributes'] =
                            'RestoreStatus,x-amz-meta-color';
                    });
                    req.on('httpData', chunk => { rawXml += chunk; });
                    req.on('error', err => reject(err));
                    req.on('success', () => {
                        parseString(rawXml, (err, parsed) => {
                            if (err) {
                                return reject(err);
                            }
                            return resolve(parsed);
                        });
                    });
                    req.send();
                });

                const result = parsedXml.ListVersionsResult;
                assert.strictEqual(result.Version.length, 1);
                assert.strictEqual(result.Version[0].Key[0], 'delete-marker-object');
                assert.deepStrictEqual(result.Version[0]['x-amz-meta-color'], ['red']);
                assert.strictEqual(result.Version[0].RestoreStatus.length, 1);

                assert.strictEqual(result.DeleteMarker.length, 1);
                assert.strictEqual(result.DeleteMarker[0].Key[0], 'delete-marker-object');
                assert.strictEqual(result.DeleteMarker[0]['x-amz-meta-color'], undefined);
                assert.strictEqual(result.DeleteMarker[0].RestoreStatus, undefined);
            });

            it('should apply prefix filter and return the user-metadata element on matching objects', async () => {
                const s3 = bucketUtil.s3;
                await s3.putObject({
                    Bucket: bucketName, Key: 'match/a', Metadata: { color: 'red' },
                }).promise();
                await s3.putObject({
                    Bucket: bucketName, Key: 'match/b', Metadata: { color: 'blue' },
                }).promise();
                await s3.putObject({
                    Bucket: bucketName, Key: 'other/c', Metadata: { color: 'green' },
                }).promise();

                const { parsedXml } = await listObjectsV2WithOptionalAttributes(
                    s3,
                    bucketName,
                    'x-amz-meta-color',
                    { Prefix: 'match/' },
                );

                const contents = parsedXml.ListBucketResult.Contents;
                assert.strictEqual(contents.length, 2);
                const keys = contents.map(c => c.Key[0]).sort();
                assert.deepStrictEqual(keys, ['match/a', 'match/b']);
                for (const c of contents) {
                    const expected = c.Key[0] === 'match/a' ? 'red' : 'blue';
                    assert.deepStrictEqual(c['x-amz-meta-color'], [expected]);
                }
            });

            it('should not attach user-metadata elements to CommonPrefixes entries', async () => {
                const s3 = bucketUtil.s3;
                await s3.putObject({
                    Bucket: bucketName, Key: 'group-a/x', Metadata: { color: 'red' },
                }).promise();
                await s3.putObject({
                    Bucket: bucketName, Key: 'group-b/y', Metadata: { color: 'blue' },
                }).promise();

                const { parsedXml } = await listObjectsV2WithOptionalAttributes(
                    s3,
                    bucketName,
                    'x-amz-meta-color',
                    { Delimiter: '/' },
                );

                const result = parsedXml.ListBucketResult;
                const prefixes = result.CommonPrefixes.map(cp => cp.Prefix[0]).sort();
                assert.deepStrictEqual(prefixes, ['group-a/', 'group-b/']);
                for (const cp of result.CommonPrefixes) {
                    assert.strictEqual(cp['x-amz-meta-color'], undefined);
                }
                assert.ok(result.Contents === undefined || result.Contents.length === 0);
            });

            it('should apply the user-metadata element consistently across paginated responses', async () => {
                const s3 = bucketUtil.s3;
                for (const k of ['p1', 'p2', 'p3']) {
                    await s3.putObject({
                        Bucket: bucketName, Key: k, Metadata: { color: 'red' },
                    }).promise();
                }

                const page1 = await listObjectsV2WithOptionalAttributes(
                    s3,
                    bucketName,
                    'x-amz-meta-color',
                    { MaxKeys: 2 },
                );
                const c1 = page1.parsedXml.ListBucketResult.Contents;
                assert.strictEqual(c1.length, 2);
                for (const c of c1) {
                    assert.deepStrictEqual(c['x-amz-meta-color'], ['red']);
                }

                const nextToken = page1.parsedXml.ListBucketResult.NextContinuationToken[0];
                assert.ok(nextToken);

                const page2 = await listObjectsV2WithOptionalAttributes(
                    s3,
                    bucketName,
                    'x-amz-meta-color',
                    { MaxKeys: 2, ContinuationToken: nextToken },
                );
                const c2 = page2.parsedXml.ListBucketResult.Contents;
                assert.strictEqual(c2.length, 1);
                assert.deepStrictEqual(c2[0]['x-amz-meta-color'], ['red']);
            });

            it('should coexist Owner with user-metadata in fetch-owner=true and keep schema order', async () => {
                const s3 = bucketUtil.s3;
                await s3.putObject({
                    Bucket: bucketName,
                    Key: 'fetch-owner-object',
                    Metadata: { color: 'red' },
                }).promise();

                const { parsedXml, rawXml } = await listObjectsV2WithOptionalAttributes(
                    s3,
                    bucketName,
                    'RestoreStatus,x-amz-meta-color',
                    { FetchOwner: true },
                );

                const contents = parsedXml.ListBucketResult.Contents;
                assert.strictEqual(contents.length, 1);
                assert.strictEqual(contents[0].Key[0], 'fetch-owner-object');
                assert.ok(Array.isArray(contents[0].Owner));
                assert.strictEqual(contents[0].Owner.length, 1);
                assert.deepStrictEqual(contents[0]['x-amz-meta-color'], ['red']);
                assert.strictEqual(contents[0].RestoreStatus.length, 1);

                const contentsBlock = rawXml.match(/<Contents>([\s\S]*?)<\/Contents>/)[1];
                const expectedOrder = [
                    'Key',
                    'LastModified',
                    'ETag',
                    'Size',
                    'Owner',
                    'RestoreStatus',
                    'x-amz-meta-color',
                    'StorageClass',
                ];
                const positions = expectedOrder.map(tag => ({
                    tag,
                    pos: contentsBlock.indexOf(`<${tag}>`),
                }));
                for (const p of positions) {
                    assert.ok(p.pos >= 0, `expected <${p.tag}> inside <Contents>`);
                }
                for (let i = 1; i < positions.length; i++) {
                    assert.ok(
                        positions[i - 1].pos < positions[i].pos,
                        `expected <${positions[i - 1].tag}> before <${positions[i].tag}>`,
                    );
                }
            });

            it('should return no Contents and no metadata elements when MaxKeys=0', async () => {
                const s3 = bucketUtil.s3;
                await s3.putObject({
                    Bucket: bucketName,
                    Key: 'will-not-list',
                    Metadata: { color: 'red' },
                }).promise();

                const { data, parsedXml } = await listObjectsV2WithOptionalAttributes(
                    s3,
                    bucketName,
                    'RestoreStatus,x-amz-meta-color',
                    { MaxKeys: 0 },
                );

                const result = parsedXml.ListBucketResult;
                assert.strictEqual(result.Contents, undefined);
                assert.strictEqual(result.RestoreStatus, undefined);
                assert.strictEqual(result['x-amz-meta-color'], undefined);
                assert.ok(!data.Contents || data.Contents.length === 0);
            });
        });

        const describeBypass = isVaultScality && internalPortBypassBP ? describe : describe.skip;
        describeBypass('x-amz-optional-attributes header', () => {
            let policyWithListBucketOnly;
            let userWithListBucketOnly;
            let s3ClientWithListBucketOnly;

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
                policyWithListBucketOnly = policyRes.Policy;
                const userRes = await iamClient.createUser({ UserName: 'user-without-permission' }).promise();
                userWithListBucketOnly = userRes.User;
                await iamClient
                    .attachUserPolicy({
                        UserName: userWithListBucketOnly.UserName,
                        PolicyArn: policyWithListBucketOnly.Arn,
                    })
                    .promise();

                const accessKeyRes = await iamClient.createAccessKey({
                    UserName: userWithListBucketOnly.UserName,
                }).promise();
                const accessKey = accessKeyRes.AccessKey;
                const s3Config = getConfig('default', {
                    credentials: new AWS.Credentials(accessKey.AccessKeyId, accessKey.SecretAccessKey),
                });
                s3ClientWithListBucketOnly = new AWS.S3(s3Config);
            });

            after(async () => {
                await iamClient
                    .detachUserPolicy({
                        UserName: userWithListBucketOnly.UserName,
                        PolicyArn: policyWithListBucketOnly.Arn,
                    })
                    .promise();
                await iamClient.deletePolicy({ PolicyArn: policyWithListBucketOnly.Arn }).promise();
                await iamClient.deleteUser({ UserName: userWithListBucketOnly.UserName }).promise();
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
                        s3ClientWithListBucketOnly,
                        Bucket,
                        'x-amz-meta-*,RestoreStatus,x-amz-meta-department',
                    );
                    throw new Error('Request should have been rejected');
                } catch (err) {
                    assert.strictEqual(err.statusCode, 403);
                    assert.strictEqual(err.code, 'AccessDenied');
                }
            });

            it('should always (ignore permission) return an XML when the header is RestoreStatus', async () => {
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
                    s3ClientWithListBucketOnly,
                    Bucket,
                    'RestoreStatus',
                );

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
                        const res = await iamClient.createPolicy({
                            PolicyName: `${userName}-policy`,
                            PolicyDocument: JSON.stringify(policyDoc),
                        }).promise();
                        policy = res.Policy;
                    }
                    const userRes = await iamClient.createUser({ UserName: userName }).promise();
                    if (policy) {
                        await iamClient.attachUserPolicy({
                            UserName: userName,
                            PolicyArn: policy.Arn,
                        }).promise();
                    }
                    const accessKeyRes = await iamClient.createAccessKey({
                        UserName: userName,
                    }).promise();
                    const ak = accessKeyRes.AccessKey;
                    const s3Cfg = getConfig('default', {
                        credentials: new AWS.Credentials(ak.AccessKeyId, ak.SecretAccessKey),
                    });
                    return { user: userRes.User, policy, s3: new AWS.S3(s3Cfg) };
                };

                const teardownIamUser = async (user, policy) => {
                    if (policy) {
                        await iamClient.detachUserPolicy({
                            UserName: user.UserName,
                            PolicyArn: policy.Arn,
                        }).promise();
                        await iamClient.deletePolicy({ PolicyArn: policy.Arn }).promise();
                    }
                    await iamClient.deleteUser({ UserName: user.UserName }).promise();
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
                        await listObjectsV2WithOptionalAttributes(
                            s3ClientOptAttrsOnly,
                            bucketName,
                            'x-amz-meta-foo',
                        );
                        throw new Error('Request should have been rejected');
                    } catch (err) {
                        assert.strictEqual(err.statusCode, 403);
                        assert.strictEqual(err.code, 'AccessDenied');
                    }
                });

                it('should reject when user has neither permission', async () => {
                    try {
                        await listObjectsV2WithOptionalAttributes(
                            s3ClientNoPermissions,
                            bucketName,
                            'x-amz-meta-foo',
                        );
                        throw new Error('Request should have been rejected');
                    } catch (err) {
                        assert.strictEqual(err.statusCode, 403);
                        assert.strictEqual(err.code, 'AccessDenied');
                    }
                });

                it('should reject when explicit deny on the new permission overrides allow', async () => {
                    try {
                        await listObjectsV2WithOptionalAttributes(
                            s3ClientListAllowAttrsDeny,
                            bucketName,
                            'x-amz-meta-foo',
                        );
                        throw new Error('Request should have been rejected');
                    } catch (err) {
                        assert.strictEqual(err.statusCode, 403);
                        assert.strictEqual(err.code, 'AccessDenied');
                    }
                });
            });

            describe('bucket policy evaluation of scality:ListBucketOptionalObjectAttributes', () => {
                let userWithBothPerms;
                let policyWithBothPerms;
                let s3ClientWithBothPerms;

                before(async () => {
                    const userName = 'user-with-both-perms';
                    const policyRes = await iamClient.createPolicy({
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
                    }).promise();
                    policyWithBothPerms = policyRes.Policy;
                    const userRes = await iamClient.createUser({ UserName: userName }).promise();
                    userWithBothPerms = userRes.User;
                    await iamClient.attachUserPolicy({
                        UserName: userName,
                        PolicyArn: policyWithBothPerms.Arn,
                    }).promise();
                    const accessKeyRes = await iamClient.createAccessKey({
                        UserName: userName,
                    }).promise();
                    const ak = accessKeyRes.AccessKey;
                    const s3Cfg = getConfig('default', {
                        credentials: new AWS.Credentials(ak.AccessKeyId, ak.SecretAccessKey),
                    });
                    s3ClientWithBothPerms = new AWS.S3(s3Cfg);
                });

                after(async () => {
                    await iamClient.detachUserPolicy({
                        UserName: userWithBothPerms.UserName,
                        PolicyArn: policyWithBothPerms.Arn,
                    }).promise();
                    await iamClient.deletePolicy({ PolicyArn: policyWithBothPerms.Arn }).promise();
                    await iamClient.deleteUser({ UserName: userWithBothPerms.UserName }).promise();
                });

                afterEach(async () => {
                    await bucketUtil.s3
                        .deleteBucketPolicy({ Bucket: bucketName })
                        .promise()
                        .catch(() => {});
                });

                // eslint-disable-next-line max-len
                it('should allow when the bucket policy supplies scality:ListBucketOptionalObjectAttributes that IAM lacks', async () => {
                    // IAM grants s3:ListBucket only; the bucket policy supplies the missing
                    // scality:ListBucketOptionalObjectAttributes so the request is allowed.
                    await bucketUtil.s3.putBucketPolicy({
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
                    }).promise();

                    await bucketUtil.s3.putObject({
                        Bucket: bucketName,
                        Key: 'object-with-color',
                        Metadata: { color: 'red' },
                    }).promise();

                    const result = await listObjectsV2WithOptionalAttributes(
                        s3ClientWithListBucketOnly,
                        bucketName,
                        'x-amz-meta-color',
                    );

                    assert.ok(Array.isArray(result.Contents));
                    assert.strictEqual(result.Contents.length, 1);
                    assert.strictEqual(result.Contents[0].Key, 'object-with-color');
                });

                it('should reject when the bucket policy denies the new action even if IAM allows it', async () => {
                    await bucketUtil.s3.putBucketPolicy({
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
                    }).promise();

                    try {
                        await listObjectsV2WithOptionalAttributes(
                            s3ClientWithBothPerms,
                            bucketName,
                            'x-amz-meta-foo',
                        );
                        throw new Error('Request should have been rejected');
                    } catch (err) {
                        assert.strictEqual(err.statusCode, 403);
                        assert.strictEqual(err.code, 'AccessDenied');
                    }
                });
            });
        });
    });
});
