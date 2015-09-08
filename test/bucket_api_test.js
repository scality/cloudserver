/*To discuss:
1) Do we need to stub to prevent side effects?
2) What about headers and AWS options?
3) Specific response/error messages if key not found, etc.?
4) Why not use AWS terminology?
*/


var chai = require("chai");
var expect = chai.expect;
var Bucket = require("../lib/bucket.js");
var sinon = require("sinon");


describe("bucket API for getting, putting and deleting objects in a bucket",function(){

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

});


describe('bucket API for getting a subset of objects from a bucket', function() {
/*	Implementation of AWS GET Bucket (List Objects) functionality
	Rules:  
		1) 	Return individual keys if key matches prefix but does not have delimeter.
		2)	Return grouped keys under common prefixes if key starts with prefix and matches other keys 
				before occurrence of delimeter.  For instance, "key2/sample" and "key2/moreSample" will be 
				grouped together if prefix is "key" and delimeter is "/".
		3)	If marker specified, only return keys that occur alphabetically AFTER the marker.
		4)	If specify limit/max-keys, will only return up to that max.  All keys grouped under common-prefix,
				will only count as one key to reach limit.  If not all keys returned due to reaching limit, 
				"IsTruncated" will be set to true and "NextMarker" will specify the last key returned in
				this search so that it can serve as the marker in the next search.  
				*/

	
 
});








