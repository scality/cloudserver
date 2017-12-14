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

const keyName = `somekeyInitMPU-${Date.now()}`;

const b2Timeout = 10000;

describeSkipIfNotMultiple('Multiple backend LIST.PART object from B2',
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

	    describe('List Part (MPU) to B2', () => {
			let uploadId = null;
			const bodyPart1 = '';
			const eTagExpectedPart1 = expectedETag(bodyPart1);
			const bodyPart2 = Buffer.alloc(10485760);
			const eTagExpectedPart2 = expectedETag(bodyPart2);
            before(done => {
                const params = {
                    Bucket: b2Location,
                    Key: keyName,
                    Metadata: { 'scal-location-constraint': b2Location },
                };
				s3.createMultipartUpload(params, (err, res) => {
					uploadId = res.UploadId;
					const params = {
	                    Bucket: b2Location,
	                    Key: keyName,
						UploadId: uploadId,
						PartNumber: 1,
						Body: bodyPart1,
	                };
					s3.uploadPart(params, (err, res) => {
						done();
					})
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

			it('should return no error && list 1 part when testing listMPU with 1 part uploaded with valid params', done => {
				const params = {
					Bucket: b2Location,
					Key: keyName,
					UploadId: uploadId,
				};
				s3.listParts(params, (err, res) => {
					assert.equal(err, null, `Expected success but got error ${err}`);
					assert.strictEqual(res.Parts.length, 1);
	                assert.strictEqual(res.Parts[0].PartNumber, 1);
	                assert.strictEqual(res.Parts[0].Size, 0);
	                assert.strictEqual(res.Parts[0].ETag, eTagExpectedPart1);
					done();
				})
			})

			it('should return no error && same MD5 when Uploading part 2', done => {
				const params = {
                    Bucket: b2Location,
                    Key: keyName,
					UploadId: uploadId,
					PartNumber: 2,
					Body: bodyPart2,
                };
				s3.uploadPart(params, (err, res) => {
					assert.equal(err, null, `Expected success but got error ${err}`);
                    assert.strictEqual(res.ETag, eTagExpectedPart2, `Expected ETag : ${eTagExpectedPart2} but got : ${res.ETag}`);
					done();
				})
			})

			it('should return no error & list 2 part when testing listMPU with 2 part uploaded with valid params', done => {
				const params = {
					Bucket: b2Location,
					Key: keyName,
					UploadId: uploadId,
				};
				s3.listParts(params, (err, res) => {
					assert.equal(err, null, `Expected success but got error ${err}`);
					assert.strictEqual(res.Parts.length, 2);
	                assert.strictEqual(res.Parts[0].PartNumber, 1);
	                assert.strictEqual(res.Parts[0].Size, 0);
	                assert.strictEqual(res.Parts[0].ETag, eTagExpectedPart1);
	                assert.strictEqual(res.Parts[1].PartNumber, 2);
	                assert.strictEqual(res.Parts[1].Size, 10485760);
	                assert.strictEqual(res.Parts[1].ETag, eTagExpectedPart2);
					done();
				})
			})
			it('should return MissingRequiredParameter when testing listPart with no key', done => {
				let tmpKey = null;
				const params = {
                    Bucket: b2Location,
                    Key: tmpKey,
					UploadId: uploadId,
                };
				s3.listParts(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, LIST.PART with no key should always throw, please run test again');
					assert.equal(err, 'MissingRequiredParameter: Missing required key \'Key\' in params', `Expected error MissingRequiredParameter but got error ${err}`);
					done();
				})
			})
			it('should return code 404 when testing listPart with non valid Key', done => {
				let tmpKey = 'PleaseDontCreateAFileWithThisNameOrThisTestWillFail-' + Date.now();
				const params = {
					Bucket: b2Location,
					Key: tmpKey,
					UploadId: uploadId,
				};
				s3.listParts(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, this Key : ' + `${tmpKey}` + ' seems to exist already, please run test again');
					assert.equal(err.statusCode, 404, `Expected error 404 but got error ${err.statusCode}`);
					done();
				})
			})
			it('should return MissingRequiredParameter when testing listMPU with no location', done => {
				let tmpLoc = null;
				const params = {
					Bucket: tmpLoc,
					Key: keyName,
					UploadId: uploadId,
				};
				s3.listParts(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, LIST.PART with no key should always throw, please run test again');
					assert.equal(err, 'MissingRequiredParameter: Missing required key \'Bucket\' in params', `Expected error MissingRequiredParameter but got error ${err}`);
					done();
				})
			})
			it('should return code 400 when testing listPart with non valid location', done => {
				let tmpLoc = 'PleaseDontCreateALocationWithThisNameOrThisTestWillFail-' + Date.now();
				const params = {
					Bucket: tmpLoc,
					Key: keyName,
					UploadId: uploadId,
				};
				s3.listParts(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, this location : ' + `${tmpLoc}` + ' seems to exist already, please run test again');
					assert.equal(err.statusCode, 400, `Expected error 400 but got error ${err.statusCode}`);
					done();
				})
			})
			it('should return MissingRequiredParameter when testing listPart with no uploadId', done => {
				let tmpUploadId = null;
				const params = {
					Bucket: b2Location,
					Key: keyName,
					UploadId: tmpUploadId,
				};
				s3.listParts(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, LIST.PART with no location should always throw, please run test again');
					assert.equal(err, 'MissingRequiredParameter: Missing required key \'UploadId\' in params', `Expected error MissingRequiredParameter but got error ${err}`);
					done();
				})
			})
			it('should return code 404 when testing listPart with non valid uploadId', done => {
				let tmpUploadId = 'PleaseDontCreateAnUploadIdWithThisIdOrThisTestWillFail-' + Date.now();
				const params = {
					Bucket: b2Location,
					Key: keyName,
					UploadId: tmpUploadId,
				};
				s3.listParts(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, this uploadId : ' + `${uploadId}` + ' seems to exist already, please run test again');
					assert.equal(err.statusCode, 404, `Expected error 404 but got error ${err.statusCode}`);
					done();
				})
			})
		})
	})
})
