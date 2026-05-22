const assert = require('assert');
const {
    CreateBucketCommand,
    CreateMultipartUploadCommand,
    UploadPartCommand,
    ListPartsCommand,
    AbortMultipartUploadCommand,
} = require('@aws-sdk/client-s3');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucket = 'bucketlistparts';
const key = 'key';
const bodyFirstPart = Buffer.allocUnsafe(10).fill(0);
const bodySecondPart = Buffer.allocUnsafe(20).fill(0);

describe('List parts', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        let uploadId;
        let secondEtag;

        beforeEach(async () => {
            await s3.send(new CreateBucketCommand({ Bucket: bucket }));
            const res = await s3.send(
                new CreateMultipartUploadCommand({
                    Bucket: bucket,
                    Key: key,
                }),
            );
            uploadId = res.UploadId;
            await s3.send(
                new UploadPartCommand({
                    Bucket: bucket,
                    Key: key,
                    PartNumber: 1,
                    UploadId: uploadId,
                    Body: bodyFirstPart,
                }),
            );
            const secondRes = await s3.send(
                new UploadPartCommand({
                    Bucket: bucket,
                    Key: key,
                    PartNumber: 2,
                    UploadId: uploadId,
                    Body: bodySecondPart,
                }),
            );
            secondEtag = secondRes.ETag;
        });

        afterEach(async () => {
            process.stdout.write('Emptying bucket');
            await s3.send(
                new AbortMultipartUploadCommand({
                    Bucket: bucket,
                    Key: key,
                    UploadId: uploadId,
                }),
            );
            await bucketUtil.empty(bucket);
            process.stdout.write('Deleting bucket');
            await bucketUtil.deleteOne(bucket);
        });

        it('should only list the second part', () =>
            s3
                .send(
                    new ListPartsCommand({
                        Bucket: bucket,
                        Key: key,
                        PartNumberMarker: '1',
                        UploadId: uploadId,
                    }),
                )
                .then(data => {
                    assert.strictEqual(data.Parts[0].PartNumber, 2);
                    assert.strictEqual(data.Parts[0].Size, 20);
                    assert.strictEqual(`${data.Parts[0].ETag}`, secondEtag);
                }));
    });
});

/** Tests for special characters in XML **/

function createPart(sigCfg, bucketUtil, s3, key) {
    let uploadId;
    return s3
        .send(new CreateBucketCommand({ Bucket: bucket }))
        .then(() =>
            s3.send(
                new CreateMultipartUploadCommand({
                    Bucket: bucket,
                    Key: key,
                }),
            ),
        )
        .then(res => {
            uploadId = res.UploadId;
            return s3.send(
                new UploadPartCommand({
                    Bucket: bucket,
                    Key: key,
                    PartNumber: 1,
                    UploadId: uploadId,
                    Body: bodyFirstPart,
                }),
            );
        })
        .then(() => Promise.resolve(uploadId));
}

function deletePart(s3, bucketUtil, key, uploadId) {
    process.stdout.write('Emptying bucket');

    return s3
        .send(
            new AbortMultipartUploadCommand({
                Bucket: bucket,
                Key: key,
                UploadId: uploadId,
            }),
        )
        .then(() => bucketUtil.empty(bucket))
        .then(() => {
            process.stdout.write('Deleting bucket');
            return bucketUtil.deleteOne(bucket);
        });
}

function testFunc(s3, bucket, key, uploadId) {
    return s3
        .send(
            new ListPartsCommand({
                Bucket: bucket,
                Key: key,
                UploadId: uploadId,
            }),
        )
        .then(data => {
            assert.strictEqual(data.Key, key);
        });
}

describe('List parts - object keys with special characters: `&`', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        let uploadId;
        const key = '&amp';

        beforeEach(() =>
            createPart(sigCfg, bucketUtil, s3, key).then(res => {
                uploadId = res;
                return Promise.resolve();
            }),
        );

        afterEach(() => deletePart(s3, bucketUtil, key, uploadId));

        it('should list parts of an object with `&` in its key', () => testFunc(s3, bucket, key, uploadId));
    });
});

describe('List parts - object keys with special characters: `"`', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        let uploadId;
        const key = '"quot';

        beforeEach(() =>
            createPart(sigCfg, bucketUtil, s3, key).then(res => {
                uploadId = res;
                return Promise.resolve();
            }),
        );

        afterEach(() => deletePart(s3, bucketUtil, key, uploadId));

        it('should list parts of an object with `"` in its key', () => testFunc(s3, bucket, key, uploadId));
    });
});

describe("List parts - object keys with special characters: `'`", () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        let uploadId;
        const key = "'apos";

        beforeEach(() =>
            createPart(sigCfg, bucketUtil, s3, key).then(res => {
                uploadId = res;
                return Promise.resolve();
            }),
        );

        afterEach(() => deletePart(s3, bucketUtil, key, uploadId));

        it("should list parts of an object with `'` in its key", () => testFunc(s3, bucket, key, uploadId));
    });
});

describe('List parts - object keys with special characters: `<`', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        let uploadId;
        const key = '<lt';

        beforeEach(() =>
            createPart(sigCfg, bucketUtil, s3, key).then(res => {
                uploadId = res;
                return Promise.resolve();
            }),
        );

        afterEach(() => deletePart(s3, bucketUtil, key, uploadId));

        it('should list parts of an object with `<` in its key', () => testFunc(s3, bucket, key, uploadId));
    });
});

describe('List parts - object keys with special characters: `>`', () => {
    withV4(sigCfg => {
        const bucketUtil = new BucketUtility('default', sigCfg);
        const s3 = bucketUtil.s3;
        let uploadId;
        const key = '>gt';

        beforeEach(() =>
            createPart(sigCfg, bucketUtil, s3, key).then(res => {
                uploadId = res;
                return Promise.resolve();
            }),
        );

        afterEach(() => deletePart(s3, bucketUtil, key, uploadId));

        it('should list parts of an object with `>` in its key', () => testFunc(s3, bucket, key, uploadId));
    });
});
