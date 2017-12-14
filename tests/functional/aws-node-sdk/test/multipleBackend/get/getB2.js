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

describeSkipIfNotMultiple('Multiple backend GET object from B2',
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
				after(done => {
					s3.deleteObject({ Bucket: b2Location, Key: testKey });
					done();
				})
				it('should return no error && same MD5 when testing GET with valid params', done => {
					s3.getObject({ Bucket: b2Location, Key: testKey }, (err, res) => {
						assert.equal(err, null, `Expected success but got error ${err}`);
						assert.strictEqual(res.ETag, `"${key.MD5}"`, `Expected identical MD5 : got ${res.ETag} , expected: ${key.MD5}`);
						done();
					});
				});
				it('should return error MissingRequiredParameter when testing GET with no Location', done => {
                    let tmpLoc = null;
					s3.getObject({ Bucket: tmpLoc, Key: testKey }, (err, res) => {
						assert.notEqual(err, null, 'Expected error but got success');
						assert.equal(err, 'MissingRequiredParameter: Missing required key \'Bucket\' in params', `Expected error MissingRequiredParameter but got error ${err}`);
						done();
					});
				});
				it('should return code 400 when testing GET with non existing Location', done => {
                    let tmpLoc = 'PleaseDontCreateALocationWithThisNameOrThisTestWillFail-' + Date.now();
					s3.getObject({ Bucket: tmpLoc, Key: testKey }, (err, res) => {
						assert.notEqual(err, null, 'Expected error but got success');
						assert.equal(err.statusCode, 400, `Expected error 400 but got error ${err.statusCode}`);
						done();
					});
				});
				it('should return error MissingRequiredParameter when testing GET with no key', done => {
					let tmpKey = null;
					s3.getObject({ Bucket: b2Location, Key: tmpKey }, (err, res) => {
						assert.notEqual(err, null, 'Expected error but got success');
						assert.equal(err, 'MissingRequiredParameter: Missing required key \'Key\' in params', `Expected error MissingRequiredParameter but got error ${err.statusCode}`);
						done();
					});
				});
				it('should return code 404 when testing GET with non existing key', done => {
					let tmpKey = 'PleaseDontCreateAFileWithThisNameOrThisTestWillFail-' + Date.now();
					s3.getObject({ Bucket: b2Location, Key: tmpKey }, (err, res) => {
						assert.notEqual(err, null, 'Expected error but got success');
						assert.equal(err.statusCode, 404, `Expected error 404 but got error ${err.statusCode}`);
						done();
					});
				});
			})
		})
        describe('with range', () => {
			const key = {
				describe: 'normal',
	            name: `somekey-${Date.now()}`,
	            body: Buffer.from('I am a body', 'utf8'),
	            MD5: 'be747eb4b75517bf6b3cf7c5fbb62f3a',
			}
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
			after(done => {
				s3.deleteObject({ Bucket: b2Location, Key: testKey });
				done();
			})
			it('should return no error when testing GET with range "bytes=0-5"', done => {
				let range = 'bytes=0-5';
				s3.getObject({ Bucket: b2Location, Key: testKey, Range: range }, (err, res) => {
					assert.equal(err, null, `Expected success but got error ${err}`);
					assert.equal(res.ContentLength, 6);
                    assert.strictEqual(res.ContentRange, 'bytes 0-5/11');
                    assert.strictEqual(res.Body.toString(), 'I am a');
					done();
				});
			});

			it('should return no error when testing GET with range "bytes=4-" not implemented yet', done => {
				assert.equal(0, 1, 'Need some review on how b2 range is working, when done uncomment code below')
				done();
			});
			// it('should return no error when testing GET with range "bytes=4-"', done => {
			// 	let range = 'bytes=4-';
			// 	s3.getObject({ Bucket: b2Location, Key: testKey, Range: range }, (err, res) => {
			// 		assert.equal(err, null, `Expected success but got error ${err}`);
			// 		assert.equal(res.ContentLength, 7);
            //         assert.strictEqual(res.ContentRange, 'bytes 4-10/11');
            //         assert.strictEqual(res.Body.toString(), ' body');
			// 		done();
			// 	});
			// });

			it('should return no error && same MD5 when testing GET with invalid range "HelloWorld"', done => {
				let range = 'HelloWorld';
				s3.getObject({ Bucket: b2Location, Key: testKey, Range: range }, (err, res) => {
					assert.equal(err, null, `Expected success but got error ${err}`);
					assert.strictEqual(res.ETag, `"${key.MD5}"`, `Expected identical MD5 : got ${res.ETag} , expected: ${key.MD5}`);
					done();
				});
			});
		})
	})
})
