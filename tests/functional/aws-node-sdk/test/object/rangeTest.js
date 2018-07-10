const { exec, execFile } = require('child_process');
const { writeFile, createReadStream } = require('fs');

const assert = require('assert');
const Promise = require('bluebird');

const withV4 = require('../support/withV4');
const BucketUtility = require('../../lib/utility/bucket-util');

const bucket = 'bucket-for-range-test';
const key = 'key-for-range-test';
let s3;

const execAsync = Promise.promisify(exec);
const execFileAsync = Promise.promisify(execFile);
const writeFileAsync = Promise.promisify(writeFile);

// Get the expected end values for various ranges (e.g., '-10', '10-', '-')
function getOuterRange(range, bytes) {
    const arr = range.split('-');
    if (arr[0] === '' && arr[1] !== '') {
        arr[0] = Number.parseInt(bytes, 10) - Number.parseInt(arr[1], 10);
        arr[1] = Number.parseInt(bytes, 10) - 1;
    } else {
        arr[0] = arr[0] === '' ? 0 : Number.parseInt(arr[0], 10);
        arr[1] = arr[1] === '' || Number.parseInt(arr[1], 10) >= bytes ?
            Number.parseInt(bytes, 10) - 1 : arr[1];
    }
    return {
        begin: arr[0],
        end: arr[1],
    };
}

// Get the ranged object from a bucket. Write the response body to a file, then
// use getRangeExec to check that all the bytes are in the correct location.
function checkRanges(range, bytes) {
    return s3.getObjectAsync({
        Bucket: bucket,
        Key: key,
        Range: `bytes=${range}`,
    })
    .then(res => {
        const { begin, end } = getOuterRange(range, bytes);
        const total = (end - begin) + 1;
        // If the range header is '-' (i.e., it is invalid), content range
        // should be undefined
        const contentRange = range === '-' ? undefined :
            `bytes ${begin}-${end}/${bytes}`;

        assert.deepStrictEqual(res.ContentLength, total.toString());
        assert.deepStrictEqual(res.ContentRange, contentRange);
        assert.deepStrictEqual(res.ContentType, 'application/octet-stream');
        assert.deepStrictEqual(res.Metadata, {});

        // Write a file using the buffer so getRangeExec can then check bytes.
        // If the getRangeExec program fails, then the range is incorrect.
        return writeFileAsync(`hashedFile.${bytes}.${range}`, res.Body)
        .then(() => execFileAsync('./getRangeExec', ['--check', '--size', total,
            '--offset', begin, `hashedFile.${bytes}.${range}`]));
    });
}

// Create 5MB parts and upload them as parts of a MPU
function uploadParts(bytes, uploadId) {
    const name = `hashedFile.${bytes}`;

    return Promise.map([1, 2], part =>
        execFileAsync('dd', [`if=${name}`, `of=${name}.mpuPart${part}`,
            'bs=5242880', `skip=${part - 1}`, 'count=1'])
        .then(() => s3.uploadPartAsync({
            Bucket: bucket,
            Key: key,
            PartNumber: part,
            UploadId: uploadId,
            Body: createReadStream(`${name}.mpuPart${part}`),
        }))
    );
}

// Create a hashed file of size bytes
function createHashedFile(bytes) {
    const name = `hashedFile.${bytes}`;
    return execFileAsync('./getRangeExec', ['--size', bytes, name]);
}

describe('aws-node-sdk range tests', () => {
    before(() => execFileAsync('gcc', ['-o', 'getRangeExec',
        'lib/utility/getRange.c']));
    after(() => execAsync('rm getRangeExec'));

    describe('aws-node-sdk range test for object put by MPU', () =>
        withV4(sigCfg => {
            const bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            const fileSize = 10 * 1024 * 1024;
            let uploadId;

            beforeEach(() =>
                s3.createBucketAsync({ Bucket: bucket })
                .then(() => s3.createMultipartUploadAsync({
                    Bucket: bucket,
                    Key: key,
                }))
                .then(res => {
                    uploadId = res.UploadId;
                })
                .then(() => createHashedFile(fileSize))
                .then(() => uploadParts(fileSize, uploadId))
                .then(res => s3.completeMultipartUploadAsync({
                    Bucket: bucket,
                    Key: key,
                    UploadId: uploadId,
                    MultipartUpload: {
                        Parts: [
                            {
                                ETag: res[0].ETag,
                                PartNumber: 1,
                            },
                            {
                                ETag: res[1].ETag,
                                PartNumber: 2,
                            },
                        ],
                    },
                }))
            );

            afterEach(() => bucketUtil.empty(bucket)
                .then(() => s3.abortMultipartUploadAsync({
                    Bucket: bucket,
                    Key: key,
                    UploadId: uploadId,
                }))
                .catch(err => new Promise((resolve, reject) => {
                    if (err.code !== 'NoSuchUpload') {
                        reject(err);
                    }
                    resolve();
                }))
                .then(() => bucketUtil.deleteOne(bucket))
                .then(() => execAsync(`rm hashedFile.${fileSize}*`))
            );

            it('should get a range from the first part of an object', () =>
                checkRanges('0-9', fileSize));

            it('should get a range from the second part of an object', () =>
                checkRanges('5242880-5242889', fileSize));

            it('should get a range that spans both parts of an object', () =>
                checkRanges('5242875-5242884', fileSize));

            it('should get a range from the second part of an object and ' +
                'include the end if the range requested goes beyond the ' +
                'actual object end', () =>
                checkRanges('10485750-10485790', fileSize));
        }));

    describe('aws-node-sdk range test of regular object put (non-MPU)', () =>
        withV4(sigCfg => {
            const bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            const fileSize = 2000;

            beforeEach(() =>
                s3.createBucketAsync({ Bucket: bucket })
                .then(() => createHashedFile(fileSize))
                .then(() => s3.putObjectAsync({
                    Bucket: bucket,
                    Key: key,
                    Body: createReadStream(`hashedFile.${fileSize}`),
                })));

            afterEach(() =>
                bucketUtil.empty(bucket)
                .then(() => bucketUtil.deleteOne(bucket))
                .then(() => execAsync(`rm hashedFile.${fileSize}*`)));

            const putRangeTests = [
                '-', // Test for invalid range
                '-1',
                '-10',
                '-512',
                '-2000',
                '0-',
                '1-',
                '190-',
                '512-',
                '0-7',
                '0-9',
                '8-15',
                '10-99',
                '0-511',
                '0-512',
                '0-513',
                '0-1023',
                '0-1024',
                '0-1025',
                '0-2000',
                '1-2000',
                '1000-1999',
                '1023-1999',
                '1024-1999',
                '1025-1999',
                '1976-1999',
                '1999-2001',
            ];

            putRangeTests.forEach(range => {
                it(`should get a range of ${range} bytes using a ${fileSize} ` +
                    'byte sized object', () =>
                    checkRanges(range, fileSize));
            });
        }));

    describe('aws-node-sdk range test for large end position', () => {
        withV4(sigCfg => {
            const bucketUtil = new BucketUtility('default', sigCfg);
            s3 = bucketUtil.s3;
            const fileSize = 2900;

            beforeEach(() =>
                s3.createBucketAsync({ Bucket: bucket })
                .then(() => createHashedFile(fileSize))
                .then(() => s3.putObjectAsync({
                    Bucket: bucket,
                    Key: key,
                    Body: createReadStream(`hashedFile.${fileSize}`),
                })));

            afterEach(() =>
                bucketUtil.empty(bucket)
                .then(() => bucketUtil.deleteOne(bucket))
                .then(() => execAsync(`rm hashedFile.${fileSize}*`)));

            it('should get the final 90 bytes of a 2890 byte object for a ' +
                'byte range of 2800-', () =>
                checkRanges('2800-', fileSize));

            it('should get the final 90 bytes of a 2890 byte object for a ' +
                'byte range of 2800-Number.MAX_SAFE_INTEGER', () =>
                checkRanges(`2800-${Number.MAX_SAFE_INTEGER}`, fileSize));
        });
    });
});
