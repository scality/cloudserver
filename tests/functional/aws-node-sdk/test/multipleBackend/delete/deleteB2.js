const assert = require('assert');

const BucketUtility = require('../../../lib/utility/bucket-util');
const withV4 = require('../../support/withV4');

const {
    describeSkipIfNotMultiple,
    getB2Keys,
    b2Location,
} = require('../utils');

const keys = getB2Keys();

const b2Timeout = 10000;

describeSkipIfNotMultiple('Multiple backend DELETE object from B2',
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
		keys.forEach(key => {
		    describe(`${key.describe} size`, () => {
				const testKey = `${key.name}-${Date.now()}`;
		        before(done => {
		            setTimeout(() => {
		                s3.putObject({
		                    Bucket: b2Location,
		                    Key: testKey,
		                    Body: key.body,
	  						Metadata: {
	  							'scal-location-constraint': b2Location,
	  						},
		                }, done);
		            }, b2Timeout);
		        });
				it('should return no error when testing DELETE with existing key', done => {
					s3.deleteObject({ Bucket: b2Location, Key: testKey }, (err, res) => {
						assert.equal(err, null, `Expected success but got error ${err}`);
						done();
					});
				});
				it('should return code 404 when testing GET with deleted key', done => {
					s3.getObject({ Bucket: b2Location, Key: testKey }, (err, res) => {
						assert.notEqual(err, null, 'Expected error but got success');
						assert.equal(err.statusCode, 404, `Expected error 404 but got error ${err.statusCode}`);
						done();
					});
				});
				it('should return MissingRequiredParameter when testing DELETE with no key', done => {
					let tmpKey = null;
					s3.deleteObject({ Bucket: b2Location, Key: tmpKey }, (err, res) => {
						assert.notEqual(err, null, 'Expected error but got success');
						assert.equal(err, 'MissingRequiredParameter: Missing required key \'Key\' in params', `Expected error MissingRequiredParameter but got error ${err}`);
						done();
					});
				});
				it('should return code 404 when testing DELETE with non existing key', done => {
					let tmpKey = 'PleaseDontCreateAFileWithThisNameOrThisTestWillFail-' + Date.now();
					s3.getObject({ Bucket: b2Location, Key: tmpKey }, (err, res) => {
						assert.notEqual(err, null, 'Expected error but got success');
						assert.equal(err.statusCode, 404, `Expected error 404 but got error ${err.statusCode}`);
						done();
					});
				});
				it('should return MissingRequiredParameter when testing DELETE with no location', done => {
					let tmpLoc = null;
					s3.deleteObject({ Bucket: tmpLoc, Key: testKey }, (err, res) => {
						assert.notEqual(err, null, 'Expected error but got success');
						assert.equal(err, 'MissingRequiredParameter: Missing required key \'Bucket\' in params', `Expected error MissingRequiredParameter but got error ${err}`);
						done();
					});
				});
				it('should return InvalidBucketName when testing DELETE with non existing location', done => {
					let tmpLoc = 'PleaseDontCreateALocationWithThisNameOrThisTestWillFail-' + Date.now();
					s3.deleteObject({ Bucket: tmpLoc, Key: testKey }, (err, res) => {
						assert.notEqual(err, null, 'Expected error but got success');
						assert.equal(err, 'InvalidBucketName: The specified bucket is not valid.', `Expected error 404 but got error ${err.statusCode}`);
						done();
					});
				});
			})
		})
	})
})
