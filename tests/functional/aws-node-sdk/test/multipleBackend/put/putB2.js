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

describeSkipIfNotMultiple('Multiple backend PUT object to B2',
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
				it('should return no error when testing PUT with valid params', done => {
                    s3.putObject({
                        Bucket: b2Location,
                        Key: testKey,
                        Body: key.body,
                        Metadata: { 'scal-location-constraint': b2Location }
                    }, (err, res) => {
						assert.equal(err, null, `Expected success but got error ${err}`);
						done();
                    });
                });
				it('should return no error && same MD5 when testing GET with valid params', done => {
					s3.getObject({ Bucket: b2Location, Key: testKey }, (err, res) => {
						assert.equal(err, null, `Expected success but got error ${err}`);
						assert.strictEqual(res.ETag, `"${key.MD5}"`, `Expected identical MD5 : got ${res.ETag} , expected: ${key.MD5}`);
						done();
					});
				});
				it('should return MissingRequiredParameter when testing PUT with no key', done => {
					let tmpKey = null;
                    s3.putObject({
                        Bucket: b2Location,
                        Key: tmpKey,
                        Body: key.body,
                        Metadata: { 'scal-location-constraint': b2Location }
                    }, (err, res) => {
						assert.notEqual(err, null, 'Expected error but got success, PUT with no key should always throw, please run test again');
                        assert.equal(err, 'MissingRequiredParameter: Missing required key \'Key\' in params', `Expected error MissingRequiredParameter but got error ${err}`);
						done();
                    });
                });
				it('should return MissingRequiredParameter when testing PUT with no location', done => {
					let tmpLoc = null;
                    s3.putObject({
                        Bucket: tmpLoc,
                        Key: testKey,
                        Body: key.body,
                        Metadata: { 'scal-location-constraint': tmpLoc }
                    }, (err, res) => {
						assert.notEqual(err, null, 'Expected error but got success, PUT with empty location should always throw, please run test again');
                        assert.equal(err, 'MissingRequiredParameter: Missing required key \'Bucket\' in params', `Expected error MissingRequiredParameter but got error ${err}`);
						done();
                    });
                });
				it('should return code 400 when testing PUT with non existing location', done => {
					let tmpLoc = 'PleaseDontCreateALocationWithThisNameOrThisTestWillFail-' + Date.now();
                    s3.putObject({
                        Bucket: tmpLoc,
                        Key: testKey,
                        Body: key.body,
                        Metadata: { 'scal-location-constraint': tmpLoc }
                    }, (err, res) => {
						assert.notEqual(err, null, 'Expected error but got success, this location seems to exist already, please run test again');
						assert.equal(err.statusCode, 400, `Expected error 400 but got error ${err.statusCode}`);
						done();
                    });
                });
				it('should return diff MD5 (if non empty size), when testing GET after PUT a corrupted empty body', done => {
					let tmpBody = null
                    s3.putObject({
                        Bucket: b2Location,
                        Key: testKey,
                        Body: tmpBody,
                        Metadata: { 'scal-location-constraint': b2Location }
                    }, (err, res) => {
						assert.equal(err, null, `Expected success but got error ${err}`);
						s3.getObject({ Bucket: b2Location, Key: testKey }, (err, res) => {
							assert.equal(err, null, `Expected success but got error ${err}`);
							if (key.describe == 'empty') {
								assert.equal(res.ETag, `"${key.MD5}"`, `Expected identicals MD5 but got : ${res.ETag} , expected : ${key.MD5}`);
							}
							else {
								assert.notEqual(res.ETag, `"${key.MD5}"`, `Expected different MD5 but got identicals: ${res.ETag}`);
							}
							done();
						});
                    });
                });
                it('should return diff MD5 when testing GET after PUT corrupted body', done => {
					let tmpBody = 'PleaseDontCreateABodyWithThisContentOrThisTestWillFail-' + Date.now();
                    s3.putObject({
                        Bucket: b2Location,
                        Key: testKey,
                        Body: tmpBody,
                        Metadata: { 'scal-location-constraint': b2Location }
                    }, (err, res) => {
						assert.equal(err, null, `Expected success but got error ${err}`);
						s3.getObject({ Bucket: b2Location, Key: testKey }, (err, res) => {
							assert.equal(err, null, `Expected success but got error ${err}`);
							assert.notEqual(res.ETag, `"${key.MD5}"`, `Expected different MD5 but got identicals: ${res.ETag}`);
							done();
						});
                    });
                });
			})
		})
	})
})
