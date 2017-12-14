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

describeSkipIfNotMultiple('Multiple backend COMPLETE.MPU object from B2',
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
			const bodyPart1 = Buffer.alloc(10485760);
			const eTagExpectedPart1 = expectedETag(bodyPart1);
			const bodyPart2 = Buffer.alloc(8485760);
			const eTagExpectedPart2 = expectedETag(bodyPart2);
			const eTagExpectedSum = '4270458b9c21bec2d62c27c78995b4e6-2';
			const partArray = []
			partArray.push({ ETag: eTagExpectedPart1, PartNumber: 1 });
			partArray.push({ ETag: eTagExpectedPart2, PartNumber: 2 });
            before(done => {
                const params = {
                    Bucket: b2Location,
                    Key: keyName,
                    Metadata: { 'scal-location-constraint': b2Location },
                };
				s3.createMultipartUpload(params, (err, res) => {
					uploadId = res.UploadId;
					const paramsPart1 = {
	                    Bucket: b2Location,
	                    Key: keyName,
						UploadId: uploadId,
						PartNumber: 1,
						Body: bodyPart1,
	                };
					s3.uploadPart(paramsPart1, (err, res) => {
						const paramsPart2 = {
		                    Bucket: b2Location,
		                    Key: keyName,
							UploadId: uploadId,
							PartNumber: 2,
							Body: bodyPart2,
		                };
						s3.uploadPart(paramsPart2, (err, res) => {
							done();
						});

					})
				})
            });
			it('should return no error & list 2 part when testing listMPU with 2 part uploaded', done => {
				const params = {
					Bucket: b2Location,
					Key: keyName,
					UploadId: uploadId,
				};
				s3.listParts(params, (err, res) => {
					assert.equal(err, null, `Expected success but got error ${err}`);
					assert.strictEqual(res.Parts.length, 2);
	                assert.strictEqual(res.Parts[0].PartNumber, 1);
	                assert.strictEqual(res.Parts[0].Size, 10485760);
	                assert.strictEqual(res.Parts[0].ETag, eTagExpectedPart1);
	                assert.strictEqual(res.Parts[1].PartNumber, 2);
	                assert.strictEqual(res.Parts[1].Size, 8485760);
	                assert.strictEqual(res.Parts[1].ETag, eTagExpectedPart2);
					done();
				})
			})
			it('should return MissingRequiredParameter when testing completeMPU with no key', done => {
				let tmpKey = null;
				const params = {
                    Bucket: b2Location,
                    Key: tmpKey,
					UploadId: uploadId,
					MultipartUpload: { Parts: partArray },
                };
				s3.completeMultipartUpload(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, COMPLETE.MPU with no key should always throw, please run test again');
					assert.equal(err, 'MissingRequiredParameter: Missing required key \'Key\' in params', `Expected error MissingRequiredParameter but got error ${err}`);
					done();
				})
			})
			it('should return code 404 when testing completeMPU with non valid key', done => {
				let tmpKey = 'PleaseDontCreateAFileWithThisNameOrThisTestWillFail-' + Date.now();
				const params = {
					Bucket: b2Location,
					Key: tmpKey,
					UploadId: uploadId,
					MultipartUpload: { Parts: partArray },
				};
				s3.completeMultipartUpload(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, this key : ' + `${tmpKey}` + ' seems to exist already, please run test again');
					assert.equal(err.statusCode, 404, `Expected error 404 but got error ${err.statusCode}`);
					done();
				})
			})
			it('should return MissingRequiredParameter when testing completeMPU with no location', done => {
				let tmpLoc = null;
				const params = {
					Bucket: tmpLoc,
					Key: keyName,
					UploadId: uploadId,
					MultipartUpload: { Parts: partArray },
				};
				s3.completeMultipartUpload(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, COMPLETE.MPU with no key should always throw, please run test again');
					assert.equal(err, 'MissingRequiredParameter: Missing required key \'Bucket\' in params', `Expected error MissingRequiredParameter but got error ${err}`);
					done();
				})
			})
			it('should return code 400 when testing completeMPU with non valid location', done => {
				let tmpLoc = 'PleaseDontCreateALocationWithThisNameOrThisTestWillFail-' + Date.now();
				const params = {
					Bucket: tmpLoc,
					Key: keyName,
					UploadId: uploadId,
					MultipartUpload: { Parts: partArray },
				};
				s3.completeMultipartUpload(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, this location : ' + `${tmpLoc}` + ' seems to exist already, please run test again');
					assert.equal(err.statusCode, 400, `Expected error 400 but got error ${err.statusCode}`);
					done();
				})
			})
			it('should return MissingRequiredParameter when testing completeMPU with no uploadId', done => {
				let tmpUploadId = null;
				const params = {
					Bucket: b2Location,
					Key: keyName,
					UploadId: tmpUploadId,
					MultipartUpload: { Parts: partArray },
				};
				s3.completeMultipartUpload(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, COMPLETE.MPU with no uploadId should always throw, please run test again');
					assert.equal(err, 'MissingRequiredParameter: Missing required key \'UploadId\' in params', `Expected error MissingRequiredParameter but got error ${err}`);
					done();
				})
			})
			it('should return code 404 when testing completeMPU with non valid uploadId', done => {
				let tmpUploadId = 'PleaseDontCreateAnUploadIdWithThisIdOrThisTestWillFail-' + Date.now();
				const params = {
					Bucket: b2Location,
					Key: keyName,
					UploadId: tmpUploadId,
					MultipartUpload: { Parts: partArray },
				};
				s3.completeMultipartUpload(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, this uploadId : ' + `${uploadId}` + ' seems to exist already, please run test again');
					assert.equal(err.statusCode, 404, `Expected error 404 but got error ${err.statusCode}`);
					done();
				})
			})
			it('should return TypeError when testing completeMPU with no MultipartUpload infos', done => {
				let tmpMultipartUpload = null;
				const params = {
					Bucket: b2Location,
					Key: keyName,
					UploadId: uploadId,
					MultipartUpload: tmpMultipartUpload,
				};
				s3.completeMultipartUpload(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, COMPLETE.MPU with no Part infos should always throw, please run test again');
					assert.equal(err, 'TypeError: Cannot read property \'Parts\' of null', `Expected error MissingRequiredParameter but got error ${err}`);
					done();
				})
			})
			it('should return MalformedXML when testing completeMPU with no MultipartUpload infos', done => {
				let tmpPartArray = null;
				const params = {
					Bucket: b2Location,
					Key: keyName,
					UploadId: uploadId,
					MultipartUpload: { Parts: tmpPartArray },
				};
				s3.completeMultipartUpload(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, COMPLETE.MPU with no Part infos should always throw, please run test again');
					assert.equal(err, 'MalformedXML: The XML you provided was not well-formed or did not validate against our published schema.', `Expected error MissingRequiredParameter but got error ${err}`);
					done();
				})
			})
			it('should return code 400 when testing completeMPU with non valid ETAG', done => {
				let tmpPartArray = [];
				let tmpEtag = 'PleaseDontCreateAnETAGWithThisIdOrThisTestWillFail-' + Date.now();
				tmpPartArray.push({ ETag: tmpEtag, PartNumber: 1 });
				tmpPartArray.push({ ETag: eTagExpectedPart2, PartNumber: 2 });
				const params = {
					Bucket: b2Location,
					Key: keyName,
					UploadId: uploadId,
					MultipartUpload: { Parts: tmpPartArray },
				};
				s3.completeMultipartUpload(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, this should\'nt happend, please run test again');
					assert.equal(err.statusCode, 400, `Expected error 400 but got error ${err.statusCode}`);
					done();
				})
			})
			it('should return code 400 when testing completeMPU with non valid PartNumber', done => {
				let tmpPartArray = [];
				tmpPartArray.push({ ETag: eTagExpectedPart2, PartNumber: 5 });
				tmpPartArray.push({ ETag: eTagExpectedPart2, PartNumber: 10 });
				const params = {
					Bucket: b2Location,
					Key: keyName,
					UploadId: uploadId,
					MultipartUpload: { Parts: tmpPartArray },
				};
				s3.completeMultipartUpload(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, this should\'nt happend, please run test again');
					assert.equal(err.statusCode, 400, `Expected error 400 but got error ${err.statusCode}`);
					done();
				})
			})
			it('should return no error when testing completeMPU with 2 part uploaded', done => {
				const params = {
					Bucket: b2Location,
					Key: keyName,
					UploadId: uploadId,
					MultipartUpload: { Parts: partArray },
				};
				s3.completeMultipartUpload(params, (err, res) => {
					assert.equal(err, null, `Expected success but got error ${err}`);
					assert.equal(res.Key, keyName, `Expected key : ${keyName} but got : ${res.Key}`);
					assert.equal(res.ETag, `"${eTagExpectedSum}"`, `Expected ETag : "${eTagExpectedSum}" but got : ${res.ETag}`);
					done();
				})
			})
		})
	})
})
