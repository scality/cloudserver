import assert from 'assert';

import withV4 from '../support/withV4';
import BucketUtility from '../../lib/utility/bucket-util';

const bucket = 'bucketlistparts';
const key = 'key';
const bodyFirstPart = Buffer.allocUnsafe(10).fill(0);
const bodySecondPart = Buffer.allocUnsafe(20).fill(0);

function checkNoError(err) {
    assert.equal(err, null,
        `Expected success, got error ${JSON.stringify(err)}`);
}

describe('List parts', () => {
    withV4(sigCfg => {
        let bucketUtil;
        let s3;
        let uploadId;
        let secondEtag;

        beforeEach(() => {
            bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            return s3.createBucketAsync({ Bucket: bucket })
            .then(() => s3.createMultipartUploadAsync({
                Bucket: bucket, Key: key }))
            .then(res => {
                uploadId = res.UploadId;
                return s3.uploadPartAsync({ Bucket: bucket, Key: key,
                  PartNumber: 1, UploadId: uploadId, Body: bodyFirstPart });
            }).then(() => s3.uploadPartAsync({ Bucket: bucket, Key: key,
                PartNumber: 2, UploadId: uploadId, Body: bodySecondPart })
            ).then(res => {
                secondEtag = res.ETag;
                return secondEtag;
            })
            .catch(err => {
                process.stdout.write(`Error in beforeEach: ${err}\n`);
                throw err;
            });
        });

        afterEach(() => {
            process.stdout.write('Emptying bucket');
            return s3.abortMultipartUploadAsync({
                Bucket: bucket, Key: key, UploadId: uploadId,
            })
            .then(() => bucketUtil.empty(bucket))
            .then(() => {
                process.stdout.write('Deleting bucket');
                return bucketUtil.deleteOne(bucket);
            })
            .catch(err => {
                process.stdout.write('Error in afterEach');
                throw err;
            });
        });

        it('should only list the second part', done => {
            s3.listParts({
                Bucket: bucket,
                Key: key,
                PartNumberMarker: 1,
                UploadId: uploadId },
            (err, data) => {
                checkNoError(err);
                assert.strictEqual(data.Parts[0].PartNumber, 2);
                assert.strictEqual(data.Parts[0].Size, 20);
                assert.strictEqual(`${data.Parts[0].ETag}`, secondEtag);
                done();
            });
        });
    });
});

/** Tests for special characters in XML **/

/* eslint-disable no-param-reassign */
function createPart(sigCfg, bucketUtil, s3, key) {
    let uploadId;
    return s3.createBucketAsync({ Bucket: bucket })
    .then(() => s3.createMultipartUploadAsync({
        Bucket: bucket, Key: key }))
    .then(res => {
        uploadId = res.UploadId;
        return s3.uploadPartAsync({ Bucket: bucket, Key: key,
          PartNumber: 1, UploadId: uploadId, Body: bodyFirstPart });
    })
    .then(() => Promise.resolve(uploadId));
}
/* eslint-enable no-param-reassign */
function deletePart(s3, bucketUtil, key, uploadId) {
    process.stdout.write('Emptying bucket');

    return s3.abortMultipartUploadAsync({
        Bucket: bucket, Key: key, UploadId: uploadId,
    })
    .then(() => bucketUtil.empty(bucket))
    .then(() => {
        process.stdout.write('Deleting bucket');
        return bucketUtil.deleteOne(bucket);
    });
}

function test(s3, bucket, key, uploadId, cb) {
    s3.listParts({
        Bucket: bucket,
        Key: key,
        UploadId: uploadId },
    (err, data) => {
        checkNoError(err);
        assert.strictEqual(data.Key, key);
        cb();
    });
}

describe('List parts - object keys with special characters: `&`', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        let uploadId;
        const key = '&amp';

        beforeEach(() =>
            createPart(sigCfg, bucketUtil, s3, key)
            .then(res => {
                uploadId = res;
                return Promise.resolve();
            })
        );

        afterEach(() => deletePart(s3, bucketUtil, key, uploadId));

        it('should list parts of an object with `&` in its key',
            done => test(s3, bucket, key, uploadId, done));
    });
});

describe('List parts - object keys with special characters: `"`', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        let uploadId;
        const key = '"quot';

        beforeEach(() =>
            createPart(sigCfg, bucketUtil, s3, key)
            .then(res => {
                uploadId = res;
                return Promise.resolve();
            })
        );

        afterEach(() => deletePart(s3, bucketUtil, key, uploadId));

        it('should list parts of an object with `"` in its key',
            done => test(s3, bucket, key, uploadId, done));
    });
});

describe('List parts - object keys with special characters: `\'`', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        let uploadId;
        const key = '\'apos';

        beforeEach(() =>
            createPart(sigCfg, bucketUtil, s3, key)
            .then(res => {
                uploadId = res;
                return Promise.resolve();
            })
        );

        afterEach(() => deletePart(s3, bucketUtil, key, uploadId));

        it('should list parts of an object with `\'` in its key',
            done => test(s3, bucket, key, uploadId, done));
    });
});

describe('List parts - object keys with special characters: `<`', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        let uploadId;
        const key = '<lt';

        beforeEach(() =>
            createPart(sigCfg, bucketUtil, s3, key)
            .then(res => {
                uploadId = res;
                return Promise.resolve();
            })
        );

        afterEach(() => deletePart(s3, bucketUtil, key, uploadId));

        it('should list parts of an object with `<` in its key',
            done => test(s3, bucket, key, uploadId, done));
    });
});

describe('List parts - object keys with special characters: `>`', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        let uploadId;
        const key = '>gt';

        beforeEach(() =>
            createPart(sigCfg, bucketUtil, s3, key)
            .then(res => {
                uploadId = res;
                return Promise.resolve();
            })
        );

        afterEach(() => deletePart(s3, bucketUtil, key, uploadId));

        it('should list parts of an object with `>` in its key',
            done => test(s3, bucket, key, uploadId, done));
    });
});
