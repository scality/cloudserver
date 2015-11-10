import {expect} from 'chai';
import async from 'async';
import Bucket from '../../../lib/bucket_mem';
import {isKeyInContents} from '../../../lib/bucket_utilities';
import { makeid, shuffle, timeDiff } from '../helpers';

describe('bucket API for getting, putting and deleting ' +
         'objects in a bucket', () => {
    let bucket;
    before(() => {
        bucket = new Bucket();
    });

    after((done) => {
        bucket.deleteBucketMD(() => {
            done();
        });
    });

    it("should create a bucket with a keyMap", (done) => {
        expect(bucket).to.be.an("object");
        expect(bucket.keyMap).to.be.an("object");
        done();
    });

    it('should be able to add an object to a bucket ' +
       'and get the object by key', (done) => {
        bucket.putObjectMD("sampleKey", "sampleValue", () => {
            bucket.getObjectMD("sampleKey", (err, value) => {
                expect(value).to.equal("sampleValue");
                done();
            });
        });
    });

    it('should return an error in response ' +
       'to getObjectMD when no such key', (done) => {
        bucket.getObjectMD('notThere', (err, value) => {
            expect(err).to.be.true;
            expect(value).to.be.undefined;
            done();
        });
    });

    it('should be able to delete an object from a bucket', (done) => {
        bucket.putObjectMD(
            'objectToDelete', 'valueToDelete', () => {
                bucket.deleteObjectMD('objectToDelete', () => {
                    bucket.getObjectMD(
                        'objectToDelete', (err, value) => {
                            expect(err).to.be.true;
                            expect(value).to.be.undefined;
                            done();
                        });
                });
            });
    });
});


describe('bucket API for getting a subset of ' +
         'objects from a bucket', () => {
    /*
     * Implementation of AWS GET Bucket (List Objects) functionality
     * Rules:
     * 1) 	Return individual key if key does not contain the
     * delimiter (even if key begins with specified prefix).
     * 2)	Return key under common prefix if key begins with
     * prefix and contains delimiter.
     * All keys that contain the same substring starting
     * with the prefix and ending with the first
     * occurrence of the delimiter will be grouped
     * together and appear once under common prefix.
     * For instance, "key2/sample" and "key2/moreSample" will be
     * grouped together under key2/ if prefix is "key" and delimiter is "/".
     * 3)	If do not specify prefix, return grouped keys under
     * common prefix if they contain
     * same substring starting at beginning of the key
     * and ending before first occurrence of delimiter.
     * 4)	There will be no grouping if no delimiter specified
     * as argument in getBucketListObjects.
     * 5)	If marker specified, only return keys that occur
     * alphabetically AFTER the marker.
     * 6)	If specify maxKeys, only return up to that max.
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

    before(() => {
        bucket = new Bucket();
    });

    after((done) => {
        bucket.deleteBucketMD(() => {
            done();
        });
    });

    it('should return individual key if key does not contain ' +
       'the delimiter even if key contains prefix', (done) => {
        async.waterfall([
            function waterfall1(next) {
                bucket.putObjectMD(
                    'key1', 'valueWithoutDelimiter', next);
            },
            function waterfall2(next) {
                bucket.putObjectMD(
                    'noMatchKey', 'non-matching key', next);
            },
            function waterfall3(next) {
                bucket.putObjectMD('key1/', 'valueWithDelimiter', next);
            },
            function waterfall4(next) {
                bucket.getBucketListObjects(
                    'key', null, delimiter, defaultLimit, next);
            }
        ],
        function waterfallFinal(err, response) {
            expect(isKeyInContents(response, 'key1')).to.be.true;
            expect(response.CommonPrefixes
                .indexOf('key1')).to.equal(-1);
            expect(isKeyInContents(response, 'key1/')).to.be.false;
            expect(response.CommonPrefixes
                .indexOf('key1/')).to.be.above(-1);
            expect(isKeyInContents(response, 'noMatchKey')).to.be.false;
            expect(response.CommonPrefixes
                .indexOf('noMatchKey')).to.equal(-1);
            done();
        });
    });

    it('should return grouped keys under common prefix if keys start with ' +
       'given prefix and contain given delimiter', (done) => {
        async.waterfall([
            function waterfall1(next) {
                bucket.putObjectMD(
                    'key/one', 'value1', next);
            },
            function waterfall2(next) {
                bucket.putObjectMD(
                    'key/two', 'value2', next);
            },
            function waterfall3(next) {
                bucket.putObjectMD('key/three', 'value3', next);
            },
            function waterfall4(next) {
                bucket.getBucketListObjects(
                    'ke', null, delimiter, defaultLimit, next);
            }
        ],
        function waterfallFinal(err, response) {
            expect(response.CommonPrefixes
                .indexOf("key/")).to.be.above(-1);
            expect(isKeyInContents(response, "key/")).to.be.false;
            done();
        });
    });

    it('should return grouped keys if no prefix ' +
       'given and keys match before delimiter', (done) => {
        bucket.putObjectMD("noPrefix/one", "value1", () => {
            bucket.putObjectMD("noPrefix/two", "value2", () => {
                bucket.getBucketListObjects(
                    null, null, delimiter, defaultLimit,
                    (err, response) => {
                        expect(response.CommonPrefixes
                            .indexOf("noPrefix/")).to.be.above(-1);
                        expect(isKeyInContents(response, "noPrefix"))
                            .to.be.false;
                        done();
                    });
            });
        });
    });

    it('should return no grouped keys if no ' +
       'delimiter specified in getBucketListObjects', (done) => {
        bucket.getBucketListObjects(
            'key', null, null, defaultLimit, (err, response) => {
                expect(response.CommonPrefixes.length).to.equal(0);
                done();
            });
    });

    it('should only return keys occurring alphabetically ' +
       'AFTER marker when no delimiter specified', (done) => {
        bucket.putObjectMD('a', 'shouldBeExcluded', () => {
            bucket.putObjectMD('b', 'shouldBeIncluded', () => {
                bucket.getBucketListObjects(
                        null, 'a', null, defaultLimit,
                        (err, response) => {
                            expect(isKeyInContents(response, 'b'))
                                .to.be.true;
                            expect(isKeyInContents(response, 'a'))
                                .to.be.false;
                            done();
                        });
            });
        });
    });

    it('should only return keys occurring alphabetically AFTER ' +
       'marker when delimiter specified', (done) => {
        bucket.getBucketListObjects(
            null, 'a', delimiter, defaultLimit,
            (err, response) => {
                expect(isKeyInContents(response, 'b')).to.be.true;
                expect(isKeyInContents(response, 'a')).to.be.false;
                done();
            });
    });

    it('should only return keys occurring alphabetically AFTER ' +
       'marker when delimiter and prefix specified', (done) => {
        bucket.getBucketListObjects(
            'b', 'a', delimiter, defaultLimit,
            (err, response) => {
                expect(isKeyInContents(response, 'b')).to.be.true;
                expect(isKeyInContents(response, 'a')).to.be.false;
                done();
            });
    });

    it('should return a NextMarker if ' +
       'maxKeys reached', (done) => {
        async.waterfall([
            function waterfall1(next) {
                bucket.putObjectMD(
                    'next/', 'shouldBeListed', next);
            },
            function waterfall2(next) {
                bucket.putObjectMD(
                    'next/rollUp', 'shouldBeRolledUp', next);
            },
            function waterfall3(next) {
                bucket.putObjectMD(
                    'next1/', 'shouldBeNextMarker', next);
            },
            function waterfall4(next) {
                bucket.getBucketListObjects(
                    'next', null, delimiter, smallLimit, next);
            }
        ],
        function waterfallFinal(err, response) {
            expect(response.CommonPrefixes
                .indexOf("next/")).to.be.above(-1);
            expect(response.CommonPrefixes
                .indexOf("next1/")).to.equal(-1);
            expect(response.NextMarker).to.equal("next1/");
            expect(response.IsTruncated).to.be.true;
            done();
        });
    });
});


describe("stress test for bucket API", function describe() {
    this.timeout(200000);

    // Test should be of at least 100,000 keys
    const numKeys = 100000;
    // We expect 1,000 puts per second
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

    before(() => {
        bucket = new Bucket();
    });

    after((done) => {
        bucket.deleteBucketMD(() => {
            done();
        });
    });

    it('should put ' + numKeys + ' keys into bucket and retrieve bucket list ' +
       'in under ' + maxMilliseconds + ' milliseconds', (done) => {
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
        let key;
        for (key in data) {
            if (data.hasOwnProperty(key)) {
                for (let k = 0; k < data[key].length; k++) {
                    keys.push(key + delimiter + data[key][k]);
                }
            }
        }

        // Shuffle the keys array so the keys appear in random order
        shuffle(keys);

        // Start timing
        const startTime = process.hrtime();

        async.each(keys, (item, next) => {
            bucket.putObjectMD(item, "value", next);
        }, (err) => {
            if (err) {
                console.error("Error" + err);
                expect(err).to.be.undefined;
                done();
            } else {
                bucket.getBucketListObjects(
                    null, null, delimiter, null, (err, response) => {
                    // Stop timing and calculate millisecond time difference
                        const diff = timeDiff(startTime);
                        expect(diff).to.be.below(maxMilliseconds);
                        prefixes.forEach((prefix) => {
                            expect(
                                response.CommonPrefixes
                                    .indexOf(prefix + delimiter))
                                        .to.be.above(-1);
                        });
                        done();
                    });
            }
        });
    });

    it('should return all keys as Contents if delimiter ' +
       'does not match and specify NextMarker', (done) => {
        bucket.getBucketListObjects(
            null, null, oddDelimiter, testLimit, (err, response) => {
                expect(response.CommonPrefixes.length).to.equal(0);
                expect(response.Contents.length).to.equal(testLimit);
                expect(response.IsTruncated).to.be.true;
                expect(response.NextMarker).to.be.a.string;
                done();
            });
    });

    it('should return only keys occurring ' +
       'after specified marker', (done) => {
        bucket.getBucketListObjects(
            null, testMarker, delimiter, null, (err, response) => {
                expect(response.CommonPrefixes.length)
                    .to.equal(prefixes.length - 1);
                expect(response.CommonPrefixes.indexOf(testPrefix))
                    .to.equal(-1);
                expect(response.Contents.length).to.equal(0);
                expect(response.IsTruncated).to.be.false;
                expect(response.NextMarker).to.be.undefined;
                done();
            });
    });
});
