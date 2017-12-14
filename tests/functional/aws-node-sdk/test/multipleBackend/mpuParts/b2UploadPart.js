const assert = require('assert');

const BucketUtility = require('../../../lib/utility/bucket-util');
const withV4 = require('../../support/withV4');

const { s3middleware } = require('arsenal');

const azureMpuUtils = s3middleware.azureHelper.mpuUtils;

const {
    describeSkipIfNotMultiple,
    getB2Keys,
    b2Location,
	expectedETag,
} = require('../utils');

const keyName = `somekeyUploadPart-${Date.now()}`;

const b2Timeout = 10000;

describeSkipIfNotMultiple('Multiple backend UPLOADPART object from B2',
function testSuite() {
	this.timeout(30000);
	withV4(sigCfg => {
	    let bucketUtil;
	    let s3;

	    before(() => {
	        process.stdout.write('Creating bucket\n');
	        bucketUtil = new BucketUtility('default', sigCfg);
	        s3 = bucketUtil.s3;
	        return s3.createBucketAsync({ Bucket: b2Location })
	        .catch(err => {
	            process.stdout.write(`Error creating bucket: ${err}\n`);
	            throw err;
	        });
	    });

	    after(() => {
	        process.stdout.write('Emptying bucket\n');
	        return bucketUtil.empty(b2Location)
	        .then(() => {
	            process.stdout.write('Deleting bucket\n');
	            return bucketUtil.deleteOne(b2Location);
	        })
	        .catch(err => {
	            process.stdout.write('Error emptying/deleting bucket: ' +
	            `${err}\n`);
	            throw err;
	        });
	    });

	    describe('Upload Part (MPU) to B2', () => {
			let uploadId = null;
            before(done => {
                const params = {
                    Bucket: b2Location,
                    Key: keyName,
                    Metadata: { 'scal-location-constraint': b2Location },
                };
				s3.createMultipartUpload(params, (err, res) => {
					uploadId = res.UploadId;
					done();
				})
            });
			after(done => {
                const params = {
                    Bucket: b2Location,
                    Key: keyName,
                    UploadId: uploadId,
                };
                s3.abortMultipartUpload(params, done);
            });
			it('should return no error && same MD5 when testing UploadPart 1 with body', done => {
				const body = Buffer.alloc(10485760);
				const eTagExpected = expectedETag(body);
				const params = {
                    Bucket: b2Location,
                    Key: keyName,
					UploadId: uploadId,
					PartNumber: 1,
					Body: body,
                };
				s3.uploadPart(params, (err, res) => {
					assert.equal(err, null, `Expected success but got error ${err}`);
                    assert.strictEqual(res.ETag, eTagExpected, `Expected ETag : ${eTagExpected} but got : ${res.ETag}`);
					done();
				})
			})

			it('should return no error && same MD5 when testing UploadPart 1 with no body', done => {
				const params = {
                    Bucket: b2Location,
                    Key: keyName,
					UploadId: uploadId,
					PartNumber: 1,
                };
				s3.uploadPart(params, (err, res) => {
					const eTagExpected = `"${azureMpuUtils.zeroByteETag}"`;
					assert.equal(err, null, `Expected success but got error ${err}`);
                    assert.strictEqual(res.ETag, eTagExpected, `Expected ETag : ${eTagExpected} but got : ${res.ETag}`);
					done();
				})
			})

			it('should return no error when testing listMPU to part 1 with valid params', done => {
				s3.listMultipartUploads({ Bucket: b2Location }, (err, res) => {
					assert.equal(err, null, `Expected success but got error ${err}`);
					assert.strictEqual(res.NextKeyMarker, keyName);
					assert.strictEqual(res.NextUploadIdMarker, uploadId);
					assert.strictEqual(res.Uploads[0].Key, keyName);
					assert.strictEqual(res.Uploads[0].UploadId, uploadId);
					done();
				})
			})

			it('should return no error && same MD5 when testing UploadPart 2 with valid params', done => {
				const params = {
					Bucket: b2Location,
					Key: keyName,
					UploadId: uploadId,
					PartNumber: 2,
				};
				s3.uploadPart(params, (err, res) => {
					const eTagExpected = `"${azureMpuUtils.zeroByteETag}"`;
					assert.equal(err, null, `Expected success but got error ${err}`);
					assert.strictEqual(res.ETag, eTagExpected, `Expected ETag : ${eTagExpected} but got : ${res.ETag}`);
					done();
				})
			})

			it('should return no error when testing listMPU to part 2 with valid params', done => {
				s3.listMultipartUploads({ Bucket: b2Location }, (err, res) => {
					assert.equal(err, null, `Expected success but got error ${err}`);
					assert.strictEqual(res.NextKeyMarker, keyName);
					assert.strictEqual(res.NextUploadIdMarker, uploadId);
					assert.strictEqual(res.Uploads[0].Key, keyName);
					assert.strictEqual(res.Uploads[0].UploadId, uploadId);
					done();
				})
			})
			it('should return MissingRequiredParameter when testing UploadPart 3 with no location', done => {
				let tmpKey = null;
				const params = {
                    Bucket: b2Location,
                    Key: tmpKey,
					UploadId: uploadId,
					PartNumber: 3,
                };
				s3.uploadPart(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, UPLOAD.PART with no key should always throw, please run test again');
					assert.equal(err, 'MissingRequiredParameter: Missing required key \'Key\' in params', `Expected error MissingRequiredParameter but got error ${err}`);
					done();
				})
			})
			it('should return MissingRequiredParameter when testing UploadPart 3 with no location', done => {
				let tmpLoc = null;
				const params = {
                    Bucket: tmpLoc,
                    Key: keyName,
					UploadId: uploadId,
					PartNumber: 3,
                };
				s3.uploadPart(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, UPLOAD.PART with no location should always throw, please run test again');
					assert.equal(err, 'MissingRequiredParameter: Missing required key \'Bucket\' in params', `Expected error MissingRequiredParameter but got error ${err}`);
					done();
				})
			})
			it('should return code 400 when testing UploadPart 3 with non valid location', done => {
				let tmpLoc = 'PleaseDontCreateALocationWithThisNameOrThisTestWillFail-' + Date.now();
				const params = {
                    Bucket: tmpLoc,
                    Key: keyName,
					UploadId: uploadId,
					PartNumber: 3,
                };
				s3.uploadPart(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, this location : ' + `${tmpLoc}` + ' seems to exist already, please run test again');
					assert.equal(err.statusCode, 400, `Expected error 400 but got error ${err.statusCode}`);
					done();
				})
			})

			it('should return MissingRequiredParameter when testing UploadPart 3 with no uploadId', done => {
				let tmpUploadId = null;
				const params = {
                    Bucket: b2Location,
                    Key: keyName,
					UploadId: tmpUploadId,
					PartNumber: 3,
                };
				s3.uploadPart(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, UPLOAD.PART with no location should always throw, please run test again');
					assert.equal(err, 'MissingRequiredParameter: Missing required key \'UploadId\' in params', `Expected error MissingRequiredParameter but got error ${err}`);
					done();
				})
			})
			it('should return code 404 when testing UploadPart 3 with non valid uploadId', done => {
				let tmpUploadId = 'PleaseDontCreateAnUploadIdWithThisIdOrThisTestWillFail-' + Date.now();
				const params = {
                    Bucket: b2Location,
                    Key: keyName,
					UploadId: tmpUploadId,
					PartNumber: 3,
                };
				s3.uploadPart(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, this uploadId : ' + `${uploadId}` + ' seems to exist already, please run test again');
					assert.equal(err.statusCode, 404, `Expected error 404 but got error ${err.statusCode}`);
					done();
				})
			})

			it('should return MissingRequiredParameter when testing UploadPart no PartNumber (null)', done => {
				let tmpPartNumber = null;
				const params = {
                    Bucket: b2Location,
                    Key: keyName,
					UploadId: uploadId,
					PartNumber: tmpPartNumber,
                };
				s3.uploadPart(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, UPLOAD.PART with no partNumber should always throw, please run test again');
					assert.equal(err, 'MissingRequiredParameter: Missing required key \'PartNumber\' in params', `Expected error MissingRequiredParameter but got error ${err}`);
					done();
				})
			})
			it('should return code 400 when testing UploadPart with non valid PartNumber (neg)', done => {
				let tmpPartNumber = -1;
				const params = {
                    Bucket: b2Location,
                    Key: keyName,
					UploadId: uploadId,
					PartNumber: tmpPartNumber,
                };
				s3.uploadPart(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, UPLOAD.PART with neg partNumber should always throw, please run test again');
					assert.equal(err.statusCode, 400, `Expected error 400 but got error ${err.statusCode}`);
					done();
				})
			})
			it('should return code 400 when testing UploadPart with non valid PartNumber (too Big)', done => {
				let tmpPartNumber = 1000000000;
				const params = {
                    Bucket: b2Location,
                    Key: keyName,
					UploadId: uploadId,
					PartNumber: tmpPartNumber,
                };
				s3.uploadPart(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, UPLOAD.PART with tooBig partNumber should always throw, please run test again');
					assert.equal(err.statusCode, 400, `Expected error 400 but got error ${err.statusCode}`);
					done();
				})
			})

		})
	})
})
