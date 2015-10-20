import {expect} from 'chai';
import async from 'async';
import Bucket from '../lib/bucket_mem.js';
import {isKeyInContents} from '../lib/bucket_utilities.js';
import makeid from './makeid.js';
import shuffle from './shuffle.js';
import timeDiff from './timeDiff.js';

describe('bucket API for getting, putting and deleting ' +
    'objects in a bucket', function () {
        let bucket;
        before(function () {
            bucket = new Bucket();
        });

        after(function (done) {
            bucket.deleteBucketMD(function () {
                done();
            });
        });

        it("should create a bucket with a keyMap", function (done) {
            expect(bucket).to.be.an("object");
            expect(bucket.keyMap).to.be.an("object");
            done();
        });

        it('should be able to add an object to a bucket ' +
            'and get the object by key', function (done) {
                bucket.putObjectMD("sampleKey", "sampleValue", function () {
                    bucket.getObjectMD("sampleKey", function (err, value) {
                        expect(value).to.equal("sampleValue");
                        done();
                    });
                });
            });

        it('should return an error in response ' +
            'to getObjectMD when no such key', function (done) {
                bucket.getObjectMD('notThere', function (err, value) {
                    expect(err).to.be.true;
                    expect(value).to.be.undefined;
                    done();
                });
            });

        it('should be able to delete an ' +
            'object from a bucket', function (done) {
                bucket.putObjectMD(
                    'objectToDelete', 'valueToDelete', function () {
                        bucket.deleteObjectMD('objectToDelete', function () {
                            bucket.getObjectMD(
                                'objectToDelete', function (err, value) {
                                    expect(err).to.be.true;
                                    expect(value).to.be.undefined;
                                    done();
                                });
                        });
                    });
            });
    });


describe('bucket API for getting a subset of ' +
    'objects from a bucket', function () {
    /*	Implementation of AWS GET Bucket (List Objects) functionality
        Rules:
        1) 	Return individual key if key does not contain the
        delimiter (even if key begins with specified prefix).
        2)	Return key under common prefix if key begins with
        prefix and contains delimiter.
        All keys that contain the same substring starting
        with the prefix and ending with the first
        occurrence of the delimiter will be grouped
        together and appear once under common prefix.
        For instance, "key2/sample" and "key2/moreSample" will be
        grouped together under key2/ if prefix is "key" and delimiter is "/".
        3)	If do not specify prefix, return grouped keys under
        common prefix if they contain
        same substring starting at beginning of the key
        and ending before first occurrence of delimiter.
        4)	There will be no grouping if no delimiter specified
        as argument in getBucketListObjects.
        5)	If marker specified, only return keys that occur
        alphabetically AFTER the marker.
        6)	If specify maxKeys, only return up to that max.
        All keys grouped under common-prefix,
        will only count as one key to reach maxKeys.
        If not all keys returned due to reaching maxKeys,
        is_truncated will be set to true and next_marker will
        specify the last key returned in
        this search so that it can serve as the marker in the next search.
        */

        // defaultLimit used in most tests to have explicit limit
        const defaultLimit = 10;

        // smallLimit used to test that truncating and nextMarker is working
        const smallLimit = 1;

        const delimiter = '/';

        let bucket;

        before(function () {
            bucket = new Bucket();
        });

        after(function (done) {
            bucket.deleteBucketMD(function () {
                done();
            });
        });

        it('should return individual key if key does not contain ' +
            'the delimiter even if key contains prefix', function (done) {
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


        it('should return grouped keys under common prefix ' +
            'if keys start with given prefix and ' +
            'contain given delimiter', function (done) {
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

        it('should return grouped keys if no prefix' +
            ' given and keys match before delimiter', function (done) {
                bucket.putObjectMD("noPrefix/one", "value1", function () {
                    bucket.putObjectMD("noPrefix/two", "value2", function () {
                        bucket.getBucketListObjects(
                            null, null, delimiter, defaultLimit,
                            function (err, response) {
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
            'delimiter specified in getBucketListObjects', function (done) {
                bucket.getBucketListObjects(
                    'key', null, null, defaultLimit, function (err, response) {
                        expect(response.CommonPrefixes.length).to.equal(0);
                        done();
                    });
            });

        it('should only return keys occurring alphabetically ' +
            'AFTER marker when no delimiter specified', function (done) {
                bucket.putObjectMD('a', 'shouldBeExcluded', function () {
                    bucket.putObjectMD('b', 'shouldBeIncluded', function () {
                        bucket.getBucketListObjects(
                                null, 'a', null, defaultLimit,
                                function (err, response) {
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
            'marker when delimiter specified', function (done) {
                bucket.getBucketListObjects(
                    null, 'a', delimiter, defaultLimit,
                    function (err, response) {
                        expect(isKeyInContents(response, 'b')).to.be.true;
                        expect(isKeyInContents(response, 'a')).to.be.false;
                        done();
                    });
            });

        it('should only return keys occurring alphabetically AFTER' +
            ' marker when delimiter and prefix specified', function (done) {
                bucket.getBucketListObjects(
                    'b', 'a', delimiter, defaultLimit,
                    function (err, response) {
                        expect(isKeyInContents(response, 'b')).to.be.true;
                        expect(isKeyInContents(response, 'a')).to.be.false;
                        done();
                    });
            });

        it('should return a NextMarker if ' +
            'maxKeys reached', function (done) {
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


describe("stress test for bucket API", function () {
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

    before(function () {
        bucket = new Bucket();
    });

    after(function (done) {
        bucket.deleteBucketMD(function () {
            done();
        });
    });

    it('should put ' + numKeys + ' keys into bucket and retrieve ' +
        'bucket list in under ' + maxMilliseconds +
        ' milliseconds', function (done) {
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

            async.each(keys, function (item, next) {
                bucket.putObjectMD(item, "value", next);
            }, function (err) {
                if (err) {
                    console.error("Error" + err);
                    expect(err).to.be.undefined;
                    done();
                } else {
                    bucket.getBucketListObjects(
                        null, null, delimiter, null, function (err, response) {
                        // Stop timing and calculate millisecond time difference
                            const diff = timeDiff(startTime);
                            expect(diff).to.be.below(maxMilliseconds);
                            prefixes.forEach(function (prefix) {
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


    it('should return all keys as Contents if delimiter' +
        ' does not match and specify NextMarker', function (done) {
            bucket.getBucketListObjects(
                null, null, oddDelimiter, testLimit, function (err, response) {
                    expect(response.CommonPrefixes.length).to.equal(0);
                    expect(response.Contents.length).to.equal(testLimit);
                    expect(response.IsTruncated).to.be.true;
                    expect(response.NextMarker).to.be.a.string;
                    done();
                });
        });

    it('should return only keys occurring ' +
        'after specified marker', function (done) {
            bucket.getBucketListObjects(
                null, testMarker, delimiter, null, function (err, response) {
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
