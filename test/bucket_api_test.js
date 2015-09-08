/*To discuss:
1) Do we need to stub to prevent side effects?
2) What about headers and AWS options?
3) Specific response/error messages if key not found, etc.?
*/


var chai = require("chai");
var expect = chai.expect;
var Bucket = require("../lib/bucket.js");
var sinon = require("sinon");


describe("bucket API",function(){

  it("should create a bucket with a keyMap", function(done){
  	var bucket = new Bucket();
  	expect(bucket).to.be.an("object");
  	expect(bucket.keyMap).to.be.an("object");
  	done()
  });

  it("should be able to add a key to a bucket synchronously", function(done){
  	var bucket = new Bucket();
  	// var stub = sinon.stub(bucket, "PUTKEY");
  	// stub("sampleKey", "sampleValue");
  	bucket.PUTKEY("sampleKey", "sampleValue");
  	expect(bucket.keyMap["sampleKey"]).to.equal("sampleValue");
  	// stub.restore();
  	done();
  });

  it("should be able to add a key to a bucket asynchronously", function(done){
  	var bucket = new Bucket();
  	bucket.putKey("sampleKey", "sampleValue", function(){
  	expect(bucket.keyMap["sampleKey"]).to.equal("sampleValue");
  	done();
  	});
  });


	it("should be able to get a key from a bucket", function(done){
		var bucket = new Bucket();
		bucket.PUTKEY("sampleKey", "sampleValue");
		bucket.getKey("sampleKey", function(err, value, key){
			expect(value).to.equal("sampleValue");
			done();
		})
	});


	it("should return an error in response to getKey when no such key", function(done){
		var bucket = new Bucket();
		bucket.PUTKEY("sampleKey", "sampleValue");
		bucket.getKey("notThere", function(err, value, key){
			expect(err).to.be.true;
			expect(value).to.be.undefined;
			done();
		})
	});

	it("should be able to delete a key from a bucket", function(done){
		var bucket = new Bucket();
		bucket.PUTKEY("sampleKey", "sampleValue");
		bucket.delKey("sampleKey", function(){
			bucket.getKey("sampleKey", function(err, value, key){
				expect(err).to.be.true;
				expect(value).to.be.undefined;
				done();
			});
		});
	});



	// it("should be able to get a key from a bucket using getPrefix AWS search"), function(done){

	// });



});


describe('getPrefix', function() {
 

 
});








