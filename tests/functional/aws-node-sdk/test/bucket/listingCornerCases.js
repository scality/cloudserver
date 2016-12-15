const AWS = require('aws-sdk');
const async = require('async');
const assert = require('assert');

import getConfig from '../support/config';

function cutAttributes(data) {
    const newContent = [];
    const newPrefixes = [];
    data.Contents.forEach(item => {
        newContent.push(item.Key);
    });
    /* eslint-disable no-param-reassign */
    data.Contents = newContent;
    data.CommonPrefixes.forEach(item => {
        newPrefixes.push(item.Prefix);
    });
    /* eslint-disable no-param-reassign */
    data.CommonPrefixes = newPrefixes;
    if (data.NextMarker === '') {
        /* eslint-disable no-param-reassign */
        delete data.NextMarker;
    }
    if (data.EncodingType === '') {
        /* eslint-disable no-param-reassign */
        delete data.EncodingType;
    }
    if (data.Delimiter === '') {
        /* eslint-disable no-param-reassign */
        delete data.Delimiter;
    }
}

const Bucket = `bucket-listing-corner-cases-${Date.now()}`;

const objects = [
    { Bucket, Key: 'Pâtisserie=中文-español-English', Body: '' },
    { Bucket, Key: 'notes/spring/1.txt', Body: '' },
    { Bucket, Key: 'notes/spring/2.txt', Body: '' },
    { Bucket, Key: 'notes/spring/march/1.txt', Body: '' },
    { Bucket, Key: 'notes/summer/1.txt', Body: '' },
    { Bucket, Key: 'notes/summer/2.txt', Body: '' },
    { Bucket, Key: 'notes/summer/august/1.txt', Body: '' },
    { Bucket, Key: 'notes/year.txt', Body: '' },
    { Bucket, Key: 'notes/yore.rs', Body: '' },
    { Bucket, Key: 'notes/zaphod/Beeblebrox.txt', Body: '' },
];

describe('Listing corner cases tests', () => {
    let s3;
    before(done => {
        const config = getConfig('default', { signatureVersion: 'v4' });
        s3 = new AWS.S3(config);
        s3.createBucket(
            { Bucket }, (err, data) => {
                if (err) {
                    done(err, data);
                }
                async.each(
                    objects, (o, next) => {
                        s3.putObject(o, (err, data) => {
                            next(err, data);
                        });
                    }, done);
            });
    });
    after(done => {
        s3.listObjects({ Bucket }, (err, data) => {
            async.each(data.Contents, (o, next) => {
                s3.deleteObject({ Bucket, Key: o.Key }, next);
            }, () => {
                s3.deleteBucket({ Bucket }, done);
            });
        });
    });
    it('should list everything', done => {
        s3.listObjects({ Bucket }, (err, data) => {
            assert.strictEqual(err, null);
            cutAttributes(data);
            assert.deepStrictEqual(data, {
                IsTruncated: false,
                Marker: '',
                Contents: [
                    objects[0].Key,
                    objects[1].Key,
                    objects[2].Key,
                    objects[3].Key,
                    objects[4].Key,
                    objects[5].Key,
                    objects[6].Key,
                    objects[7].Key,
                    objects[8].Key,
                    objects[9].Key,
                ],
                Name: Bucket,
                Prefix: '',
                MaxKeys: 1000,
                CommonPrefixes: [],
            });
            done();
        });
    });
    it('should list with valid marker', done => {
        s3.listObjects(
            { Bucket,
              Delimiter: '/',
              Marker: 'notes/summer/1.txt',
            },
            (err, data) => {
                assert.strictEqual(err, null);
                cutAttributes(data);
                assert.deepStrictEqual(data, {
                    IsTruncated: false,
                    Marker: 'notes/summer/1.txt',
                    Contents: [],
                    Name: Bucket,
                    Prefix: '',
                    Delimiter: '/',
                    MaxKeys: 1000,
                    CommonPrefixes: [],
                });
                done();
            });
    });
    it('should list with unexpected marker', done => {
        s3.listObjects(
            { Bucket,
              Delimiter: '/',
              Marker: 'zzzz',
            },
            (err, data) => {
                assert.strictEqual(err, null);
                assert.deepStrictEqual(data, {
                    IsTruncated: false,
                    Marker: 'zzzz',
                    Contents: [],
                    Name: Bucket,
                    Prefix: '',
                    Delimiter: '/',
                    MaxKeys: 1000,
                    CommonPrefixes: [],
                });
                done();
            });
    });
    it('should list with unexpected marker and prefix', done => {
        s3.listObjects(
            { Bucket,
              Delimiter: '/',
              Marker: 'notes/summer0',
              Prefix: 'notes/summer/',
            },
            (err, data) => {
                assert.strictEqual(err, null);
                assert.deepStrictEqual(data, {
                    IsTruncated: false,
                    Marker: 'notes/summer0',
                    Contents: [],
                    Name: Bucket,
                    Prefix: 'notes/summer/',
                    Delimiter: '/',
                    MaxKeys: 1000,
                    CommonPrefixes: [],
                });
                done();
            });
    });
    it('should list with MaxKeys', done => {
        s3.listObjects(
            { Bucket,
              MaxKeys: 3,
            },
            (err, data) => {
                assert.strictEqual(err, null);
                cutAttributes(data);
                assert.deepStrictEqual(data, {
                    Marker: '',
                    IsTruncated: true,
                    Contents: [objects[0].Key,
                               objects[1].Key,
                               objects[2].Key,
                              ],
                    Name: Bucket,
                    Prefix: '',
                    MaxKeys: 3,
                    CommonPrefixes: [],
                });
                done();
            });
    });
    it('should list with big MaxKeys', done => {
        s3.listObjects(
            { Bucket,
              MaxKeys: 15000,
            },
            (err, data) => {
                assert.strictEqual(err, null);
                cutAttributes(data);
                assert.deepStrictEqual(data, {
                    Marker: '',
                    IsTruncated: false,
                    Contents: [objects[0].Key,
                               objects[1].Key,
                               objects[2].Key,
                               objects[3].Key,
                               objects[4].Key,
                               objects[5].Key,
                               objects[6].Key,
                               objects[7].Key,
                               objects[8].Key,
                               objects[9].Key,
                              ],
                    Name: Bucket,
                    Prefix: '',
                    MaxKeys: 15000,
                    CommonPrefixes: [],
                });
                done();
            });
    });
    it('should list with delimiter', done => {
        s3.listObjects(
            { Bucket,
              Delimiter: '/',
            },
            (err, data) => {
                assert.strictEqual(err, null);
                cutAttributes(data);
                assert.deepStrictEqual(data, {
                    Marker: '',
                    IsTruncated: false,
                    Contents: [objects[0].Key],
                    Name: Bucket,
                    Prefix: '',
                    Delimiter: '/',
                    MaxKeys: 1000,
                    CommonPrefixes: ['notes/'],
                });
                done();
            });
    });
    it('should list with long delimiter', done => {
        s3.listObjects(
            { Bucket,
              Delimiter: 'notes/summer',
            },
            (err, data) => {
                assert.strictEqual(err, null);
                cutAttributes(data);
                assert.deepStrictEqual(data, {
                    Marker: '',
                    IsTruncated: false,
                    Contents: [objects[0].Key,
                               objects[1].Key,
                               objects[2].Key,
                               objects[3].Key,
                               objects[7].Key,
                               objects[8].Key,
                               objects[9].Key,
                              ],
                    Name: Bucket,
                    Prefix: '',
                    Delimiter: 'notes/summer',
                    MaxKeys: 1000,
                    CommonPrefixes: ['notes/summer'],
                });
                done();
            });
    });
    it('should list with delimiter and prefix related to #147', done => {
        s3.listObjects(
            { Bucket,
              Delimiter: '/',
              Prefix: 'notes/',
            },
            (err, data) => {
                assert.strictEqual(err, null);
                cutAttributes(data);
                assert.deepStrictEqual(data, {
                    Marker: '',
                    IsTruncated: false,
                    Contents: [
                        objects[7].Key,
                        objects[8].Key,
                    ],
                    Name: Bucket,
                    Prefix: 'notes/',
                    Delimiter: '/',
                    MaxKeys: 1000,
                    CommonPrefixes: [
                        'notes/spring/',
                        'notes/summer/',
                        'notes/zaphod/',
                    ],
                });
                done();
            });
    });
    it('should list with prefix and marker related to #147', done => {
        s3.listObjects(
            { Bucket,
              Delimiter: '/',
              Prefix: 'notes/',
              Marker: 'notes/year.txt',
            },
            (err, data) => {
                assert.strictEqual(err, null);
                cutAttributes(data);
                assert.deepStrictEqual(data, {
                    Marker: 'notes/year.txt',
                    IsTruncated: false,
                    Contents: [objects[8].Key],
                    Name: Bucket,
                    Prefix: 'notes/',
                    Delimiter: '/',
                    MaxKeys: 1000,
                    CommonPrefixes: ['notes/zaphod/'],
                });
                done();
            });
    });
    it('should list with all parameters 1 of 5', done => {
        s3.listObjects(
            { Bucket,
              Delimiter: '/',
              Prefix: 'notes/',
              Marker: 'notes/',
              MaxKeys: 1,
            },
            (err, data) => {
                assert.strictEqual(err, null);
                cutAttributes(data);
                assert.deepStrictEqual(data, {
                    Marker: 'notes/',
                    NextMarker: 'notes/spring/',
                    IsTruncated: true,
                    Contents: [],
                    Name: Bucket,
                    Prefix: 'notes/',
                    Delimiter: '/',
                    MaxKeys: 1,
                    CommonPrefixes: ['notes/spring/'],
                });
                done();
            });
    });
    it('should list with all parameters 2 of 5', done => {
        s3.listObjects(
            { Bucket,
              Delimiter: '/',
              Prefix: 'notes/',
              Marker: 'notes/spring/',
              MaxKeys: 1,
            },
            (err, data) => {
                assert.strictEqual(err, null);
                cutAttributes(data);
                assert.deepStrictEqual(data, {
                    Marker: 'notes/spring/',
                    NextMarker: 'notes/summer/',
                    IsTruncated: true,
                    Contents: [],
                    Name: Bucket,
                    Prefix: 'notes/',
                    Delimiter: '/',
                    MaxKeys: 1,
                    CommonPrefixes: ['notes/summer/'],
                });
                done();
            });
    });
    it('should list with all parameters 3 of 5', done => {
        s3.listObjects(
            { Bucket,
              Delimiter: '/',
              Prefix: 'notes/',
              Marker: 'notes/summer/',
              MaxKeys: 1,
            },
            (err, data) => {
                assert.strictEqual(err, null);
                cutAttributes(data);
                assert.deepStrictEqual(data, {
                    Marker: 'notes/summer/',
                    NextMarker: 'notes/year.txt',
                    IsTruncated: true,
                    Contents: ['notes/year.txt'],
                    Name: Bucket,
                    Prefix: 'notes/',
                    Delimiter: '/',
                    MaxKeys: 1,
                    CommonPrefixes: [],
                });
                done();
            });
    });
    it('should list with all parameters 4 of 5', done => {
        s3.listObjects(
            { Bucket,
              Delimiter: '/',
              Prefix: 'notes/',
              Marker: 'notes/year.txt',
              MaxKeys: 1,
            },
            (err, data) => {
                assert.strictEqual(err, null);
                cutAttributes(data);
                assert.deepStrictEqual(data, {
                    Marker: 'notes/year.txt',
                    NextMarker: 'notes/yore.rs',
                    IsTruncated: true,
                    Contents: ['notes/yore.rs'],
                    Name: Bucket,
                    Prefix: 'notes/',
                    Delimiter: '/',
                    MaxKeys: 1,
                    CommonPrefixes: [],
                });
                done();
            });
    });
    it('should list with all parameters 5 of 5', done => {
        s3.listObjects(
            { Bucket,
              Delimiter: '/',
              Prefix: 'notes/',
              Marker: 'notes/yore.rs',
              MaxKeys: 1,
            },
            (err, data) => {
                assert.strictEqual(err, null);
                cutAttributes(data);
                assert.deepStrictEqual(data, {
                    Marker: 'notes/yore.rs',
                    IsTruncated: false,
                    Contents: [],
                    Name: Bucket,
                    Prefix: 'notes/',
                    Delimiter: '/',
                    MaxKeys: 1,
                    CommonPrefixes: ['notes/zaphod/'],
                });
                done();
            });
    });
    it('should ends listing on last common prefix', done => {
        s3.putObject({
            Bucket,
            Key: 'notes/zaphod/TheFourth.txt',
            Body: '',
        }, err => {
            if (!err) {
                s3.listObjects(
                    { Bucket,
                      Delimiter: '/',
                      Prefix: 'notes/',
                      Marker: 'notes/yore.rs',
                      MaxKeys: 1,
                    },
                    (err, data) => {
                        assert.strictEqual(err, null);
                        cutAttributes(data);
                        assert.deepStrictEqual(data, {
                            IsTruncated: false,
                            Marker: 'notes/yore.rs',
                            Contents: [],
                            Name: Bucket,
                            Prefix: 'notes/',
                            Delimiter: '/',
                            MaxKeys: 1,
                            CommonPrefixes: ['notes/zaphod/'],
                        });
                        done();
                    });
            }
        });
    });
});
