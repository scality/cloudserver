import assert from 'assert';
import async from 'async';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

import { removeAllVersions } from '../../lib/utility/versioning-util';

const bucket = `versioning-bucket-${Date.now()}`;

const resultElements = [
    'VersionId',
    'IsLatest',
    'LastModified',
    'Owner',
];
const versionResultElements = [
    'ETag',
    'Size',
    'StorageClass',
];

function _assertResultElements(entry, type) {
    const elements = type === 'DeleteMarker' ? resultElements :
        resultElements.concat(versionResultElements);
    elements.forEach(elem => {
        assert.notStrictEqual(entry[elem], undefined,
            `Expected ${elem} in result but did not find it`);
        if (elem === 'Owner') {
            assert(entry.Owner.ID, 'Expected Owner ID but did not find it');
            assert(entry.Owner.DisplayName,
                'Expected Owner DisplayName but did not find it');
        }
    });
}

describe('listObject - Delimiter version', function testSuite() {
    this.timeout(600000);

    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;

        // setup test
        before(done => {
            s3.createBucket({ Bucket: bucket }, done);
        });

        // delete bucket after testing
        after(done => {
            removeAllVersions({ Bucket: bucket }, err => {
                if (err) {
                    return done(err);
                }
                return s3.deleteBucket({ Bucket: bucket }, err => {
                    assert.strictEqual(err, null,
                        `Error deleting bucket: ${err}`);
                    return done();
                });
            });
        });

        let versioning = false;

        const objects = [
            { name: 'notes/summer/august/1.txt', value: 'foo', isNull: true },
            { name: 'notes/year.txt', value: 'foo', isNull: true },
            { name: 'notes/yore.rs', value: 'foo', isNull: true },
            { name: 'notes/zaphod/Beeblebrox.txt', value: 'foo', isNull: true },
            { name: 'Pâtisserie=中文-español-English', value: 'foo' },
            { name: 'Pâtisserie=中文-español-English', value: 'bar' },
            { name: 'notes/spring/1.txt', value: 'qux' },
            { name: 'notes/spring/1.txt', value: 'foo' },
            { name: 'notes/spring/1.txt', value: 'bar' },
            { name: 'notes/spring/2.txt', value: 'foo' },
            { name: 'notes/spring/2.txt', value: null },
            { name: 'notes/spring/march/1.txt', value: 'foo' },
            { name: 'notes/spring/march/1.txt', value: 'bar', isNull: true },
            { name: 'notes/summer/1.txt', value: 'foo' },
            { name: 'notes/summer/1.txt', value: 'bar' },
            { name: 'notes/summer/2.txt', value: 'bar' },
            { name: 'notes/summer/4.txt', value: null },
            { name: 'notes/summer/4.txt', value: null },
            { name: 'notes/summer/4.txt', value: null },
            { name: 'notes/summer/444.txt', value: null },
            { name: 'notes/summer/44444.txt', value: null },
        ];

        it('put objects inside bucket', done => {
            async.eachSeries(objects, (obj, next) => {
                async.waterfall([
                    next => {
                        if (!versioning && obj.isNull !== true) {
                            const params = {
                                Bucket: bucket,
                                VersioningConfiguration: {
                                    Status: 'Enabled',
                                },
                            };
                            versioning = true;
                            return s3.putBucketVersioning(params,
                                err => next(err));
                        } else if (versioning && obj.isNull === true) {
                            const params = {
                                Bucket: bucket,
                                VersioningConfiguration: {
                                    Status: 'Suspended',
                                },
                            };
                            versioning = false;
                            return s3.putBucketVersioning(params,
                                err => next(err));
                        }
                        return next();
                    },
                    next => {
                        if (obj.value === null) {
                            return s3.deleteObject({
                                Bucket: bucket,
                                Key: obj.name,
                            }, function test(err) {
                                const headers = this.httpResponse.headers;
                                assert.strictEqual(
                                    headers['x-amz-delete-marker'],
                                    'true');
                                // eslint-disable-next-line no-param-reassign
                                obj.versionId = headers['x-amz-version-id'];
                                return next(err);
                            });
                        }
                        return s3.putObject({
                            Bucket: bucket,
                            Key: obj.name,
                            Body: obj.value,
                        }, (err, res) => {
                            if (err) {
                                return next(err);
                            }
                            // eslint-disable-next-line no-param-reassign
                            obj.versionId = res.VersionId || 'null';
                            return next();
                        });
                    },
                ], err => next(err));
            }, err => done(err));
        });

        [
            {
                name: 'basic listing',
                params: {},
                expectedResult: objects,
                commonPrefix: [],
                isTruncated: false,
                nextKeyMarker: undefined,
                nextVersionIdMarker: undefined,
            },
            {
                name: 'with valid key marker',
                params: { KeyMarker: 'notes/spring/1.txt' },
                expectedResult: [
                    objects[0],
                    objects[1],
                    objects[2],
                    objects[3],
                    objects[9],
                    objects[10],
                    objects[11],
                    objects[12],
                    objects[13],
                    objects[14],
                    objects[15],
                    objects[16],
                    objects[17],
                    objects[18],
                    objects[19],
                    objects[20],
                ],
                commonPrefix: [],
                isTruncated: false,
                nextKeyMarker: undefined,
                nextVersionIdMarker: undefined,
            },
            {
                name: 'with bad key marker',
                params: { KeyMarker: 'zzzz', Delimiter: '/' },
                expectedResult: [],
                commonPrefix: [],
                isTruncated: false,
                nextKeyMarker: undefined,
                nextVersionIdMarker: undefined,
            },
            {
                name: 'with maxKeys',
                params: { MaxKeys: 3 },
                expectedResult: [
                    objects[4],
                    objects[5],
                    objects[8],
                ],
                commonPrefix: [],
                isTruncated: true,
                nextKeyMarker: objects[8].name,
                nextVersionIdMarker: objects[8],
            },
            {
                name: 'with big maxKeys',
                params: { MaxKeys: 15000 },
                expectedResult: objects,
                commonPrefix: [],
                isTruncated: false,
                nextKeyMarker: undefined,
                nextVersionIdMarker: undefined,
            },
            {
                name: 'with delimiter',
                params: { Delimiter: '/' },
                expectedResult: objects.slice(4, 6),
                commonPrefix: ['notes/'],
                isTruncated: false,
                nextKeyMarker: undefined,
                nextVersionIdMarker: undefined,
            },
            {
                name: 'with long delimiter',
                params: { Delimiter: 'notes/summer' },
                expectedResult: objects.filter(obj =>
                    obj.name.indexOf('notes/summer') < 0),
                commonPrefix: ['notes/summer'],
                isTruncated: false,
                nextKeyMarker: undefined,
                nextVersionIdMarker: undefined,
            },
            {
                name: 'bad key marker and good prefix',
                params: {
                    Delimiter: '/',
                    Prefix: 'notes/summer/',
                    KeyMarker: 'notes/summer0',
                },
                expectedResult: [],
                commonPrefix: [],
                isTruncated: false,
                nextKeyMarker: undefined,
                nextVersionIdMarker: undefined,
            },
            {
                name: 'delimiter and prefix (related to #147)',
                params: { Delimiter: '/', Prefix: 'notes/' },
                expectedResult: [
                    objects[1],
                    objects[2],
                ],
                commonPrefix: [
                    'notes/spring/',
                    'notes/summer/',
                    'notes/zaphod/',
                ],
                isTruncated: false,
                nextKeyMarker: undefined,
                nextVersionIdMarker: undefined,
            },
            {
                name: 'delimiter, prefix and marker (related to #147)',
                params: {
                    Delimiter: '/',
                    Prefix: 'notes/',
                    KeyMarker: 'notes/year.txt',
                },
                expectedResult: [objects[2]],
                commonPrefix: ['notes/zaphod/'],
                isTruncated: false,
                nextKeyMarker: undefined,
                nextVersionIdMarker: undefined,
            },
            {
                name: 'all parameters 1/5',
                params: {
                    Delimiter: '/',
                    Prefix: 'notes/',
                    KeyMarker: 'notes/',
                    MaxKeys: 1,
                },
                expectedResult: [],
                commonPrefix: ['notes/spring/'],
                isTruncated: true,
                nextKeyMarker: 'notes/spring/',
                nextVersionIdMarker: undefined,
            },
            {
                name: 'all parameters 2/5',
                params: {
                    Delimiter: '/',
                    Prefix: 'notes/',
                    KeyMarker: 'notes/spring/',
                    MaxKeys: 1,
                },
                expectedResult: [],
                commonPrefix: ['notes/summer/'],
                isTruncated: true,
                nextKeyMarker: 'notes/summer/',
                nextVersionIdMarker: undefined,
            },
            {
                name: 'all parameters 3/5',
                params: {
                    Delimiter: '/',
                    Prefix: 'notes/',
                    KeyMarker: 'notes/summer/',
                    MaxKeys: 1,
                },
                expectedResult: [objects[1]],
                commonPrefix: [],
                isTruncated: true,
                nextKeyMarker: objects[1].name,
                nextVersionIdMarker: objects[1],
            },
            {
                name: 'all parameters 4/5',
                params: {
                    Delimiter: '/',
                    Prefix: 'notes/',
                    KeyMarker: 'notes/year.txt',
                    MaxKeys: 1,
                },
                expectedResult: [objects[2]],
                commonPrefix: [],
                isTruncated: true,
                nextKeyMarker: objects[2].name,
                nextVersionIdMarker: objects[2],
            },
            {
                name: 'all parameters 5/5',
                params: {
                    Delimiter: '/',
                    Prefix: 'notes/',
                    KeyMarker: 'notes/yore.rs',
                    MaxKeys: 1,
                },
                expectedResult: [],
                commonPrefix: ['notes/zaphod/'],
                isTruncated: false,
                nextKeyMarker: undefined,
                nextVersionIdMarker: undefined,
            },
        ].forEach(test => {
            it(test.name, done => {
                const expectedResult = test.expectedResult;
                s3.listObjectVersions(
                    Object.assign({ Bucket: bucket }, test.params),
                    (err, res) => {
                        if (err) {
                            return done(err);
                        }
                        res.Versions.forEach(result => {
                            const item = expectedResult.find(obj => {
                                if (obj.name === result.Key &&
                                    obj.versionId === result.VersionId &&
                                    obj.value !== null) {
                                    return true;
                                }
                                return false;
                            });
                            if (!item) {
                                throw new Error('listing fail, ' +
                                    `unexpected key ${result.Key} ` +
                                    `with version ${result.VersionId}`);
                            }
                            _assertResultElements(result, 'Version');
                        });
                        res.DeleteMarkers.forEach(result => {
                            const item = expectedResult.find(obj => {
                                if (obj.name === result.Key &&
                                    obj.versionId === result.VersionId &&
                                    obj.value === null) {
                                    return true;
                                }
                                return false;
                            });
                            if (!item) {
                                throw new Error('listing fail, ' +
                                    `unexpected key ${result.Key} ` +
                                    `with version ${result.VersionId}`);
                            }
                            _assertResultElements(result, 'DeleteMarker');
                        });
                        res.CommonPrefixes.forEach(cp => {
                            if (!test.commonPrefix.find(
                                item => item === cp.Prefix)) {
                                throw new Error('listing fail, ' +
                                    `unexpected prefix ${cp.Prefix}`);
                            }
                        });
                        assert.strictEqual(res.IsTruncated, test.isTruncated);
                        assert.strictEqual(res.NextKeyMarker,
                            test.nextKeyMarker);
                        if (!test.nextVersionIdMarker) {
                            // eslint-disable-next-line no-param-reassign
                            test.nextVersionIdMarker = {};
                        }
                        assert.strictEqual(res.NextVersionIdMarker,
                            test.nextVersionIdMarker.versionId);
                        return done();
                    });
            });
        });
    });
});
