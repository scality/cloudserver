const assert = require('assert');
const async = require('async');

const { errors } = require('arsenal');

const BucketInfo = require('arsenal').models.BucketInfo;
const { cleanup, DummyRequestLogger } = require('../helpers');
const { isKeyInContents }
    = require('arsenal').storage.metadata.inMemory.bucketUtilities;
const metadata = require('../metadataswitch');
const { makeid, shuffle, timeDiff } = require('../helpers');

const bucketName = 'Zaphod';
const objMD = { test: '8' };
const log = new DummyRequestLogger();

describe('bucket API for getting, putting and deleting ' +
         'objects in a bucket', () => {
    let bucket;
    beforeAll(done => {
        cleanup();
        const creationDate = new Date().toJSON();
        bucket = new BucketInfo(bucketName, 'iAmTheOwnerId',
            'iAmTheOwnerDisplayName', creationDate);
        metadata.createBucket(bucketName, bucket, log, done);
    });

    test('should be able to add an object to a bucket ' +
       'and get the object by key', done => {
        metadata.putObjectMD(bucketName, 'sampleKey', objMD, {}, log, () => {
            metadata.getObjectMD(bucketName, 'sampleKey', {}, log,
            (err, value) => {
                assert.deepStrictEqual(value, objMD);
                done();
            });
        });
    });

    test('should return an error in response ' +
       'to getObjectMD when no such key', done => {
        metadata.getObjectMD(bucketName, 'notThere', {}, log, (err, value) => {
            assert.deepStrictEqual(err, errors.NoSuchKey);
            expect(value).toBe(undefined);
            done();
        });
    });

    test('should be able to delete an object from a bucket', done => {
        metadata.putObjectMD(bucketName, 'objectToDelete', '{}', {}, log,
        () => {
            metadata.deleteObjectMD(bucketName, 'objectToDelete', {}, log,
            () => {
                metadata.getObjectMD(bucketName, 'objectToDelete', {}, log,
                    (err, value) => {
                        assert.deepStrictEqual(err, errors.NoSuchKey);
                        expect(value).toBe(undefined);
                        done();
                    });
            });
        });
    });
});


describe('bucket API for getting a subset of objects from a bucket', () => {
    /*
     * Implementation of AWS GET Bucket (List Objects) functionality
     * Rules:
     * 1) Return individual key if key does not contain the
     * delimiter (even if key begins with specified prefix).
     * 2) Return key under common prefix if key begins with
     * prefix and contains delimiter.
     * All keys that contain the same substring starting
     * with the prefix and ending with the first
     * occurrence of the delimiter will be grouped
     * together and appear once under common prefix.
     * For instance, "key2/sample" and "key2/moreSample" will be
     * grouped together under key2/ if prefix is "key" and delimiter is "/".
     * 3) If do not specify prefix, return grouped keys under
     * common prefix if they contain
     * same substring starting at beginning of the key
     * and ending before first occurrence of delimiter.
     * 4) There will be no grouping if no delimiter specified
     * as argument in getBucketListObjects.
     * 5) If marker specified, only return keys that occur
     * alphabetically AFTER the marker.
     * 6) If specify maxKeys, only return up to that max.
     * All keys grouped under common-prefix,
     * will only count as one key to reach maxKeys.
     * If not all keys returned due to reaching maxKeys,
     * is_truncated will be set to true and next_marker will
     * specify the last key returned in
     * this search so that it can serve as the marker in the next search.
     */

    // defaultLimit used in most tests to have explicit limit
    const defaultLimit = 10;

    // smallLimit used to test that truncating and nextMarker is working
    const smallLimit = 1;

    const delimiter = '/';

    let bucket;

    beforeAll(done => {
        cleanup();
        const creationDate = new Date().toJSON();
        bucket = new BucketInfo(bucketName, 'ownerid',
            'ownerdisplayname', creationDate);
        metadata.createBucket(bucketName, bucket, log, done);
    });

    test('should return individual key if key does not contain ' +
       'the delimiter even if key contains prefix', done => {
        async.waterfall([
            next =>
                metadata.putObjectMD(bucketName, 'key1', '{}', {}, log, next),
            (data, next) =>
                metadata.putObjectMD(bucketName, 'noMatchKey', '{}', {}, log,
                next),
            (data, next) =>
                metadata.putObjectMD(bucketName, 'key1/', '{}', {}, log, next),
            (data, next) =>
                metadata.listObject(bucketName, { prefix: 'key', delimiter,
                    maxKeys: defaultLimit }, log, next),
        ], (err, response) => {
            expect(isKeyInContents(response, 'key1')).toBe(true);
            expect(response.CommonPrefixes.indexOf('key1')).toBe(-1);
            expect(isKeyInContents(response, 'key1/')).toBe(false);
            expect(response.CommonPrefixes.indexOf('key1/') > -1).toBeTruthy();
            expect(isKeyInContents(response, 'noMatchKey')).toBe(false);
            expect(response.CommonPrefixes.indexOf('noMatchKey')).toBe(-1);
            done();
        });
    });

    test('should return grouped keys under common prefix if keys start with ' +
       'given prefix and contain given delimiter', done => {
        async.waterfall([
            next =>
                metadata.putObjectMD(bucketName, 'key/one', '{}', {}, log,
                next),
            (data, next) =>
                metadata.putObjectMD(bucketName, 'key/two', '{}', {}, log,
                next),
            (data, next) =>
                metadata.putObjectMD(bucketName, 'key/three', '{}', {}, log,
                next),
            (data, next) =>
                metadata.listObject(bucketName, { prefix: 'ke', delimiter,
                    maxKeys: defaultLimit }, log, next),
        ], (err, response) => {
            expect(response.CommonPrefixes.indexOf('key/') > -1).toBeTruthy();
            expect(isKeyInContents(response, 'key/')).toBe(false);
            done();
        });
    });

    test('should return grouped keys if no prefix ' +
       'given and keys match before delimiter', done => {
        metadata.putObjectMD(bucketName, 'noPrefix/one', '{}', {}, log, () => {
            metadata.putObjectMD(bucketName, 'noPrefix/two', '{}', {}, log,
            () => {
                metadata.listObject(bucketName, { delimiter,
                    maxKeys: defaultLimit }, log, (err, response) => {
                        expect(response.CommonPrefixes.indexOf('noPrefix/')
                               > -1).toBeTruthy();
                        expect(isKeyInContents(response,
                                                           'noPrefix')).toBe(false);
                        done();
                    });
            });
        });
    });

    test('should return no grouped keys if no ' +
       'delimiter specified in getBucketListObjects', done => {
        metadata.listObject(bucketName,
            { prefix: 'key', maxKeys: defaultLimit }, log,
            (err, response) => {
                expect(response.CommonPrefixes.length).toBe(0);
                done();
            });
    });

    test('should only return keys occurring alphabetically ' +
       'AFTER marker when no delimiter specified', done => {
        metadata.putObjectMD(bucketName, 'a', '{}', {}, log, () => {
            metadata.putObjectMD(bucketName, 'b', '{}', {}, log, () => {
                metadata.listObject(bucketName,
                    { marker: 'a', maxKeys: defaultLimit },
                    log, (err, response) => {
                        expect(isKeyInContents(response, 'b')).toBeTruthy();
                        expect(isKeyInContents(response, 'a')).toBe(false);
                        done();
                    });
            });
        });
    });

    test('should only return keys occurring alphabetically AFTER ' +
       'marker when delimiter specified', done => {
        metadata.listObject(bucketName,
            { marker: 'a', delimiter, maxKeys: defaultLimit },
            log, (err, response) => {
                expect(isKeyInContents(response, 'b')).toBeTruthy();
                expect(isKeyInContents(response, 'a')).toBe(false);
                done();
            });
    });

    test('should only return keys occurring alphabetically AFTER ' +
       'marker when delimiter and prefix specified', done => {
        metadata.listObject(bucketName,
            { prefix: 'b', marker: 'a', delimiter, maxKeys: defaultLimit },
            log, (err, response) => {
                expect(isKeyInContents(response, 'b')).toBeTruthy();
                expect(isKeyInContents(response, 'a')).toBe(false);
                done();
            });
    });
    // Next marker should be the last common prefix or contents key returned
    test('should return a NextMarker if maxKeys reached', done => {
        async.waterfall([
            next =>
                metadata.putObjectMD(bucketName, 'next/', '{}', {}, log, next),
            (data, next) =>
                metadata.putObjectMD(bucketName, 'next/rollUp', '{}', {}, log,
                    next),
            (data, next) =>
                metadata.putObjectMD(bucketName, 'next1/', '{}', {}, log, next),
            (data, next) =>
                metadata.listObject(bucketName,
                    { prefix: 'next', delimiter, maxKeys: smallLimit },
                    log, next),
        ], (err, response) => {
            expect(response.CommonPrefixes.indexOf('next/') > -1).toBeTruthy();
            expect(response.CommonPrefixes.indexOf('next1/')).toBe(-1);
            expect(response.NextMarker).toBe('next/');
            expect(response.IsTruncated).toBeTruthy();
            done();
        });
    });
});


describe('stress test for bucket API', () => {
    this.timeout(200000);

    // Test should be of at least 100,000 keys
    const numKeys = 100000;
    // We assert.strictEqual 1,000 puts per second
    const maxMilliseconds = numKeys;

    const prefixes = ['dogs', 'cats', 'tigers', 'elephants', 'monsters'];

    // testPrefix is used to test alphabetical marker so
    // must be string alphabetized before testMarker
    const testPrefix = prefixes[1];
    const testMarker = 'cz';

    // testLimit is used to test that setting a
    // limit will result in a truncated result with a nextMarker set
    const testLimit = 1000;
    const delimiter = '/';
    const oddDelimiter = '$';
    let bucket;

    beforeAll(done => {
        cleanup();
        const creationDate = new Date().toJSON();
        bucket = new BucketInfo(bucketName, 'ownerid',
            'ownerdisplayname', creationDate);
        metadata.createBucket(bucketName, bucket, log, done);
    });

    test(`should put ${numKeys} keys into bucket and retrieve bucket list ` +
       `in under ${maxMilliseconds} milliseconds`, done => {
        const data = {};
        const keys = [];

        // Create dictionary entries based on prefixes array
        for (let i = 0; i < prefixes.length; i++) {
            data[prefixes[i]] = [];
        }
        // Populate dictionary with random key extensions
        let prefix;
        for (let j = 0; j < numKeys; j++) {
            prefix = prefixes[j % prefixes.length];
            data[prefix].push(makeid(10));
        }

        // Populate keys array with all keys including prefixes
        Object.keys(data).forEach(dkey => {
            data[dkey].forEach(key => {
                keys.push(dkey + delimiter + key);
            });
        });

        // Shuffle the keys array so the keys appear in random order
        shuffle(keys);

        // Start timing
        const startTime = process.hrtime();

        async.each(keys, (item, next) => {
            metadata.putObjectMD(bucketName, item, '{}', {}, log, next);
        }, err => {
            if (err) {
                expect(err).toBe(undefined);
                done();
            } else {
                metadata.listObject(bucketName, { delimiter },
                    log, (err, response) => {
                    // Stop timing and calculate millisecond time difference
                        const diff = timeDiff(startTime);
                        expect(diff < maxMilliseconds).toBeTruthy();
                        prefixes.forEach(prefix => {
                            expect(response.CommonPrefixes
                                   .indexOf(prefix + delimiter) > -1).toBeTruthy();
                        });
                        done();
                    });
            }
        });
    });

    test('should return all keys as Contents if delimiter ' +
       'does not match and specify NextMarker', done => {
        metadata.listObject(bucketName,
            { delimiter: oddDelimiter, maxKeys: testLimit },
            log, (err, response) => {
                expect(response.CommonPrefixes.length).toBe(0);
                expect(response.Contents.length).toBe(testLimit);
                expect(response.IsTruncated).toBe(true);
                expect(typeof response.NextMarker).toBe('string');
                done();
            });
    });

    test('should return only keys occurring ' +
       'after specified marker', done => {
        metadata.listObject(bucketName, { marker: testMarker, delimiter }, log,
            (err, res) => {
                expect(res.CommonPrefixes.length).toBe(prefixes.length - 1);
                expect(res.CommonPrefixes.indexOf(testPrefix)).toBe(-1);
                expect(res.Contents.length).toBe(0);
                expect(res.IsTruncated).toBe(false);
                expect(res.NextMarker).toBe(undefined);
                done();
            });
    });
});
