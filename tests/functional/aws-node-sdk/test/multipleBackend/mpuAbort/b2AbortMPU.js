const assert = require('assert');

const BucketUtility = require('../../../lib/utility/bucket-util');
const withV4 = require('../../support/withV4');

const { s3middleware } = require('arsenal');

const azureMpuUtils = s3middleware.azureHelper.mpuUtils;

const {
    describeSkipIfNotMultiple,
    getB2Keys,
    b2Location,
} = require('../utils');

const keyName = `somekeyInitMPU-${Date.now()}`;

const b2Timeout = 10000;

describeSkipIfNotMultiple('Multiple backend ABORT.MPU object from B2',
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
			const bodyPart = Buffer.alloc(10485760);
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
						Body: bodyPart,
	                };
					s3.uploadPart(params, (err, res) => {
						done();
					})
				})
            });

			it('should return MissingRequiredParameter when testing abortMpu with no key', done => {
				let tmpKey = null;
				const params = {
                    Bucket: b2Location,
                    Key: tmpKey,
					UploadId: uploadId,
                };
				s3.abortMultipartUpload(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, ABORT.MPU with no key should always throw, please run test again');
					assert.equal(err, 'MissingRequiredParameter: Missing required key \'Key\' in params', `Expected error MissingRequiredParameter but got error ${err}`);
					done();
				})
			})
			it('should return code 404 when testing abortMpu with non valid location', done => {
				let tmpKey = 'PleaseDontCreateAFileWithThisNameOrThisTestWillFail-' + Date.now();
				const params = {
					Bucket: b2Location,
					Key: tmpKey,
					UploadId: uploadId,
				};
				s3.abortMultipartUpload(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, this location : ' + `${tmpKey}` + ' seems to exist already, please run test again');
					assert.equal(err.statusCode, 404, `Expected error 404 but got error ${err.statusCode}`);
					done();
				})
			})
			it('should return MissingRequiredParameter when testing abortMpu with no location', done => {
				let tmpLoc = null;
				const params = {
					Bucket: tmpLoc,
					Key: keyName,
					UploadId: uploadId,
				};
				s3.abortMultipartUpload(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, ABORT.MPU with no location should always throw, please run test again');
					assert.equal(err, 'MissingRequiredParameter: Missing required key \'Bucket\' in params', `Expected error MissingRequiredParameter but got error ${err}`);
					done();
				})
			})
			it('should return code 400 when testing abortMpu with non valid location', done => {
				let tmpLoc = 'PleaseDontCreateALocationWithThisNameOrThisTestWillFail-' + Date.now();
				const params = {
					Bucket: tmpLoc,
					Key: keyName,
					UploadId: uploadId,
				};
				s3.abortMultipartUpload(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, this location : ' + `${tmpLoc}` + ' seems to exist already, please run test again');
					assert.equal(err.statusCode, 400, `Expected error 400 but got error ${err.statusCode}`);
					done();
				})
			})
			it('should return MissingRequiredParameter when testing abortMpu with no uploadId', done => {
				let tmpUploadId = null;
				const params = {
					Bucket: b2Location,
					Key: keyName,
					UploadId: tmpUploadId,
				};
				s3.abortMultipartUpload(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, UPLOAD.PART with no UploadId should always throw, please run test again');
					assert.equal(err, 'MissingRequiredParameter: Missing required key \'UploadId\' in params', `Expected error MissingRequiredParameter but got error ${err}`);
					done();
				})
			})
			it('should return code 404 when testing abortMpu with non valid uploadId', done => {
				let tmpUploadId = 'PleaseDontCreateAnUploadIdWithThisIdOrThisTestWillFail-' + Date.now();
				const params = {
					Bucket: b2Location,
					Key: keyName,
					UploadId: tmpUploadId,
				};
				s3.abortMultipartUpload(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, this uploadId : ' + `${uploadId}` + ' seems to exist already, please run test again');
					assert.equal(err.statusCode, 404, `Expected error 404 but got error ${err.statusCode}`);
					done();
				})
			})

			it('should return no error when testing abortMPU with valid params', done => {
				const params = {
					Bucket: b2Location,
					Key: keyName,
					UploadId: uploadId,
				};
				s3.abortMultipartUpload(params, (err, res) => {
					assert.equal(err, null, `Expected success but got error ${err}`);
					done();
				})
			})

			it('should return error when MPU already aborted', done => {
				const params = {
					Bucket: b2Location,
					Key: keyName,
					UploadId: uploadId,
				};
				s3.abortMultipartUpload(params, (err, res) => {
                    assert.notEqual(err, null, 'Expected error but got success, UploadId should have been abort : ' + `${uploadId}`);
					assert.equal(err.statusCode, 404, `Expected error 404 but got error ${err.statusCode}`);
					done();
				})
			})

			it('should return code 404 when testing listPart with aborted MPU', done => {
				const params = {
					Bucket: b2Location,
					Key: keyName,
					UploadId: uploadId,
				};
				s3.listParts(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success res = \n${res}\nUploadId should have been abort : ' + `${uploadId}`);
					assert.equal(err.statusCode, 404, `Expected error 404 but got error ${err.statusCode}`);
					done();
				})
			})
		})
	})
})
