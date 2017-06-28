const assert = require('assert');

const { bucketPut } = require('../../../lib/api/bucketPut');
const objectPut = require('../../../lib/api/objectPut');
const objectPutTagging = require('../../../lib/api/objectPutTagging');
const { _validator, parseTagXml }
    = require('arsenal').s3middleware.tagging;
const { cleanup,
    DummyRequestLogger,
    makeAuthInfo,
    TaggingConfigTester }
    = require('../helpers');
const metadata = require('../../../lib/metadata/wrapper');
const { taggingTests }
    = require('../../functional/aws-node-sdk/lib/utility/tagging.js');
const DummyRequest = require('../DummyRequest');

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const namespace = 'default';
const bucketName = 'bucketname';
const objectName = 'objectName';
const postBody = Buffer.from('I am a body', 'utf8');
const testBucketPutRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
};

const testPutObjectRequest = new DummyRequest({
    bucketName,
    namespace,
    objectKey: objectName,
    headers: {},
    url: `/${bucketName}/${objectName}`,
}, postBody);

function _checkError(err, code, errorName) {
    assert(err, 'Expected error but found none');
    assert.strictEqual(err.code, code);
    assert(err[errorName]);
}

function _generateSampleXml(key, value) {
    const xml = '<Tagging>' +
      '<TagSet>' +
         '<Tag>' +
           `<Key>${key}</Key>` +
           `<Value>${value}</Value>` +
         '</Tag>' +
      '</TagSet>' +
    '</Tagging>';

    return xml;
}

describe('putObjectTagging API', () => {
    beforeEach(done => {
        cleanup();
        bucketPut(authInfo, testBucketPutRequest, log, err => {
            if (err) {
                return done(err);
            }
            return objectPut(authInfo, testPutObjectRequest, undefined, log,
              done);
        });
    });

    afterEach(() => cleanup());

    it('should update an object\'s metadata with tags resource', done => {
        const taggingUtil = new TaggingConfigTester();
        const testObjectPutTaggingRequest = taggingUtil
            .createObjectTaggingRequest('PUT', bucketName, objectName);
        objectPutTagging(authInfo, testObjectPutTaggingRequest, log, err => {
            if (err) {
                process.stdout.write(`Err putting object tagging ${err}`);
                return done(err);
            }
            return metadata.getObjectMD(bucketName, objectName, {}, log,
            (err, objectMD) => {
                if (err) {
                    process.stdout.write(`Err retrieving object MD ${err}`);
                    return done(err);
                }
                const uploadedTags = objectMD.tags;
                assert.deepStrictEqual(uploadedTags, taggingUtil.getTags());
                return done();
            });
        });
    });
});

describe('PUT object tagging :: helper validation functions ', () => {
    describe('validateTagStructure ', () => {
        it('should return expected true if tag is valid false/undefined if not',
        done => {
            const tags = [
                { tagTest: { Key: ['foo'], Value: ['bar'] }, isValid: true },
                { tagTest: { Key: ['foo'] }, isValid: false },
                { tagTest: { Value: ['bar'] }, isValid: false },
                { tagTest: { Keys: ['foo'], Value: ['bar'] }, isValid: false },
                { tagTest: { Key: ['foo', 'boo'], Value: ['bar'] },
                  isValid: false },
                { tagTest: { Key: ['foo'], Value: ['bar', 'boo'] },
                  isValid: false },
                { tagTest: { Key: ['foo', 'boo'], Value: ['bar', 'boo'] },
                  isValid: false },
                { tagTest: { Key: ['foo'], Values: ['bar'] }, isValid: false },
                { tagTest: { Keys: ['foo'], Values: ['bar'] }, isValid: false },
            ];

            for (let i = 0; i < tags.length; i++) {
                const tag = tags[i];
                const result = _validator.validateTagStructure(tag.tagTest);
                if (tag.isValid) {
                    assert(result);
                } else {
                    assert(!result);
                }
            }
            done();
        });

        describe('validateXMLStructure ', () => {
            it('should return expected true if tag is valid false/undefined ' +
            'if not', done => {
                const tags = [
                    { tagging: { Tagging: { TagSet: [{ Tag: [] }] } }, isValid:
                    true },
                    { tagging: { Tagging: { TagSet: [''] } }, isValid: true },
                    { tagging: { Tagging: { TagSet: [] } }, isValid: false },
                    { tagging: { Tagging: { TagSet: [{}] } }, isValid: false },
                    { tagging: { Tagging: { Tagset: [{ Tag: [] }] } }, isValid:
                    false },
                    { tagging: { Tagging: { Tagset: [{ Tag: [] }] },
                    ExtraTagging: 'extratagging' }, isValid: false },
                    { tagging: { Tagging: { Tagset: [{ Tag: [] }], ExtraTagset:
                    'extratagset' } }, isValid: false },
                    { tagging: { Tagging: { Tagset: [{ Tag: [] }], ExtraTagset:
                    'extratagset' } }, isValid: false },
                    { tagging: { Tagging: { Tagset: [{ Tag: [], ExtraTag:
                    'extratag' }] } }, isValid: false },
                    { tagging: { Tagging: { Tagset: [{ Tag: {} }] } }, isValid:
                    false },
                ];

                for (let i = 0; i < tags.length; i++) {
                    const tag = tags[i];
                    const result = _validator.validateXMLStructure(tag.tagging);
                    if (tag.isValid) {
                        assert(result);
                    } else {
                        assert(!result);
                    }
                }
                done();
            });
        });
    });

    describe('parseTagXml', () => {
        it('should parse a correct xml', done => {
            const xml = _generateSampleXml('foo', 'bar');
            parseTagXml(xml, log, (err, result) => {
                assert.strictEqual(err, null, `Found unexpected err ${err}`);
                assert.strictEqual(result.foo, 'bar');
                return done();
            });
        });

        taggingTests.forEach(taggingTest => {
            it(taggingTest.it, done => {
                const key = taggingTest.tag.key;
                const value = taggingTest.tag.value;
                const xml = _generateSampleXml(key, value);
                parseTagXml(xml, log, (err, result) => {
                    if (taggingTest.error) {
                        _checkError(err, 400, taggingTest.error);
                    } else {
                        assert.ifError(err, `Found unexpected err ${err}`);
                        assert.deepStrictEqual(result[key], value);
                    }
                    return done();
                });
            });
        });
    });
});
