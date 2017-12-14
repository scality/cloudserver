const assert = require('assert');

const BucketUtility = require('../../../lib/utility/bucket-util');
const withV4 = require('../../support/withV4');

const {
    describeSkipIfNotMultiple,
    getB2Keys,
    b2Location,
} = require('../utils');

const keyName = `somekeyInitMPU-${Date.now()}`;

const b2Timeout = 10000;

describeSkipIfNotMultiple('Multiple backend INITMPU object to B2',
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
	    describe('Initiate MPU to B2', () => {
			let uploadId = null;
			after(done => {
                const params = {
                    Bucket: b2Location,
                    Key: keyName,
                    UploadId: uploadId,
                };
                s3.abortMultipartUpload(params, done);
            });
			it('should return no error && same Location & Key when testing initMPU with valid params', done => {
				const params = {
                    Bucket: b2Location,
                    Key: keyName,
                    Metadata: { 'scal-location-constraint': b2Location },
                };
				s3.createMultipartUpload(params, (err, res) => {
					uploadId = res.UploadId;
					assert.equal(err, null, `Expected success but got error ${err}`);
					assert.strictEqual(res.Bucket, b2Location);
					assert.strictEqual(res.Key, keyName);
					done();
				})
			})

            it('should return no error when testing listMPU with valid params', done => {
				s3.listMultipartUploads({ Bucket: b2Location }, (err, res) => {
					assert.equal(err, null, `Expected success but got error ${err}`);
					assert.strictEqual(res.NextKeyMarker, keyName);
					assert.strictEqual(res.NextUploadIdMarker, uploadId);
					assert.strictEqual(res.Uploads[0].Key, keyName);
					assert.strictEqual(res.Uploads[0].UploadId, uploadId);
					done();
				})
			})
			it('should return MissingRequiredParameter when testing initMPU with no key', done => {
				let tmpKey = null;
				const params = {
                    Bucket: b2Location,
                    Key: tmpKey,
                    Metadata: { 'scal-location-constraint': b2Location },
                };
				s3.createMultipartUpload(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, INIT.MPU with no key should always throw, please run test again');
					assert.equal(err, 'MissingRequiredParameter: Missing required key \'Key\' in params', `Expected error MissingRequiredParameter but got error ${err}`);
					done();
				})
			})
			it('should return MissingRequiredParameter when testing initMPU with no location', done => {
				let tmpLoc = null;
				const params = {
                    Bucket: tmpLoc,
                    Key: keyName,
                    Metadata: { 'scal-location-constraint': tmpLoc },
                };
				s3.createMultipartUpload(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, INIT.MPU with no location should always throw, please run test again');
					assert.equal(err, 'MissingRequiredParameter: Missing required key \'Bucket\' in params', `Expected error MissingRequiredParameter but got error ${err}`);
					done();
				})
			})
			it('should return code 400 when testing initMPU with non valid location', done => {
				let tmpLoc = 'PleaseDontCreateALocationWithThisNameOrThisTestWillFail-' + Date.now();
				const params = {
                    Bucket: tmpLoc,
                    Key: keyName,
                    Metadata: { 'scal-location-constraint': tmpLoc },
                };
				s3.createMultipartUpload(params, (err, res) => {
					assert.notEqual(err, null, 'Expected error but got success, this location : ' + `${tmpLoc}` + ' seems to exist already, please run test again');
					assert.equal(err.statusCode, 400, `Expected error 400 but got error ${err.statusCode}`);
					done();
				})
			})
		})
	})
})
