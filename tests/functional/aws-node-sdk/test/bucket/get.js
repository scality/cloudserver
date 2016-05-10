import assert from 'assert';
import tv4 from 'tv4';
import Promise from 'bluebird';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';
import bucketSchema from '../../schema/bucket';

describe('GET Bucket - AWS.S3.listObjects', () => {
    describe('When user is unauthorized', () => {
        let bucketUtil;
        let bucketName;

        before(done => {
            bucketUtil = new BucketUtility();
            bucketUtil.createRandom(1).catch(done)
                      .then(created => {
                          bucketName = created;
                          done();
                      });
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
                      .catch(done)
                      .then(created => {
                          bucketName = created;
                          done();
                      });
        });

        after(done => {
            bucketUtil.deleteOne(bucketName).then(() => done()).catch(done);
        });

        afterEach(done => {
            bucketUtil.empty(bucketName).catch(done).done(() => done());
        });

        it('should return created objects in alphabetical order', done => {
            const s3 = bucketUtil.s3;
            const Bucket = bucketName;
            const objects = [
                { Bucket, Key: 'testB/' },
                { Bucket, Key: 'testB/test.json', Body: '{}' },
                { Bucket, Key: 'testA/' },
                { Bucket, Key: 'testA/test.json', Body: '{}' },
                { Bucket, Key: 'testA/test/test.json', Body: '{}' },
            ];

            Promise
                .mapSeries(objects, param => s3.putObjectAsync(param))
                .then(() => s3.listObjectsAsync({ Bucket }))
                .then(data => {
                    const isValidResponse = tv4.validate(data, bucketSchema);
                    if (!isValidResponse) {
                        throw new Error(tv4.error);
                    }
                    return data;
                }).then(data => {
                    const keys = data.Contents.map(object => object.Key);
                    assert.equal(data.Name, Bucket, 'Bucket name mismatch');
                    assert.deepEqual(keys, [
                        'testA/',
                        'testA/test.json',
                        'testA/test/test.json',
                        'testB/',
                        'testB/test.json',
                    ], 'Bucket content mismatch');
                    done();
                }).catch(done);
        });

        it('should list object titles with white spaces', done => {
            const s3 = bucketUtil.s3;
            const Bucket = bucketName;
            const objects = [
                { Bucket, Key: 'whiteSpace/' },
                { Bucket, Key: 'whiteSpace/one whiteSpace', Body: '{}' },
                { Bucket, Key: 'whiteSpace/two white spaces', Body: '{}' },
                { Bucket, Key: 'white space/' },
                { Bucket, Key: 'white space/one whiteSpace', Body: '{}' },
                { Bucket, Key: 'white space/two white spaces', Body: '{}' },
            ];

            Promise
                .mapSeries(objects, param => s3.putObjectAsync(param))
                .then(() => s3.listObjectsAsync({ Bucket }))
                .then(data => {
                    const isValidResponse = tv4.validate(data, bucketSchema);
                    if (!isValidResponse) {
                        throw new Error(tv4.error);
                    }
                    return data;
                }).then(data => {
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
                    done();
                }).catch(done);
        });

        it('should list object titles that begin with special chars', done => {
            const s3 = bucketUtil.s3;
            const Bucket = bucketName;
            const objects = [
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
            ];

            Promise
                .mapSeries(objects, param => s3.putObjectAsync(param))
                .then(() => s3.listObjectsAsync({ Bucket }))
                .then(data => {
                    const isValidResponse = tv4.validate(data, bucketSchema);
                    if (!isValidResponse) {
                        throw new Error(tv4.error);
                    }
                    return data;
                }).then(data => {
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
                    done();
                }).catch(done);
        });
    });
});
