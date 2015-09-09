var chai = require("chai");
var expect = chai.expect;
var Bucket = require("../lib/bucket.js");


describe("bucket API for getting, putting and deleting objects in a bucket",function(){

	var bucket;

	before(function() {
	  bucket = new Bucket();
	});

	after(function(done) {
		bucket.DELETEBucket(function(){
			done();
		});
	});

  it("should create a bucket with a keyMap", function(done){
  	expect(bucket).to.be.an("object");
  	expect(bucket.keyMap).to.be.an("object");
  	done()
  });

  it("should be able to add an object to a bucket and get the object by key", function(done){
  	bucket.PUTObject("sampleKey", "sampleValue", function(){
  		bucket.GETObject("sampleKey", function(err, value, key){
  			expect(value).to.equal("sampleValue");
  			done();
  		})
  	});
  });

	it("should return an error in response to GETObject when no such key", function(done){
		bucket.GETObject("notThere", function(err, value, key){
			expect(err).to.be.true;
			expect(value).to.be.undefined;
			done();
		})
	});

	it("should be able to delete an object from a bucket", function(done){

		bucket.PUTObject("objectToDelete", "valueToDelete", function(){
			bucket.DELETEObject("objectToDelete", function(){
				bucket.GETObject("objectToDelete", function(err, value, key){
					expect(err).to.be.true;
					expect(value).to.be.undefined;
					done();
				});
			});
		});
	});


});


describe('bucket API for getting a subset of objects from a bucket', function() {
/*	Implementation of AWS GET Bucket (List Objects) functionality
	Rules:  
		1) 	Return individual key if key does not contain the delimeter (even if key begins with specified prefix).
		2)	Return key under common prefix if key begins with prefix and contains delimeter.  All 
				keys that contain the same substring starting with the prefix and ending with the first 
				occurrence of the delimeter will be grouped together and appear once under common prefix.  
				For instance, "key2/sample" and "key2/moreSample" will be 
				grouped together if prefix is "key" and delimeter is "/".  
		3)	If do not specify prefix, return grouped keys under common prefix if they contain 
				same substring starting at beginning of the key and ending before first occurrence of delimeter.
		4)	There will be no grouping if no delimeter specified as argument in GETBucketListObjects.		
		5)	If marker specified, only return keys that occur alphabetically AFTER the marker.
		6)	If specify maxKeys, only return up to that max.  All keys grouped under common-prefix,
				will only count as one key to reach maxKeys.  If not all keys returned due to reaching maxKeys, 
				is_truncated will be set to true and next_marker will specify the last key returned in
				this search so that it can serve as the marker in the next search.  
				*/


	var bucket;
			
	before(function() {
	  bucket = new Bucket();
	});

	after(function(done) {
		bucket.DELETEBucket(function(){
			done();
		});
	});

	it("should return individual key if key begins with prefix but key does not contain the delimeter", function(done){
		bucket.PUTObject("key1", "valueWithoutDelimeter", function(){
			bucket.PUTObject("noMatchKey", "non-matching key", function(){
				bucket.PUTObject("key1/", "valueWithDelimiter", function(){
					bucket.GETBucketListObjects("key", null, "/", 10, function(response){
						expect(response.fetched["key1"]).to.equal("valueWithoutDelimeter");
						expect(response.fetched["key1/"]).to.be.undefined;
						expect(response.attrs.common_prefixes.indexOf("key1/")).to.be.above(-1);
						expect(response.fetched["noMatchKey"]).to.be.undefined;
						expect(response.attrs.common_prefixes.indexOf("noMatchKey")).to.equal(-1);
						done();	
					});
				});
			});
		});
	});


	it("should return grouped keys under common prefix if key starts with given prefix and contains given delimeter", function(done){
		bucket.PUTObject("key/one", "value1", function(){
			bucket.PUTObject("key/two", "value2", function(){
				bucket.PUTObject("key/three", "value2", function(){
					bucket.GETBucketListObjects("ke", null, "/", 10, function(response){
						expect(response.attrs.common_prefixes.indexOf("key/")).to.be.above(-1);
						expect(response.fetched["key/"]).to.be.undefined;
						done();
					});
				});
			});
		});
	});

	it("should return grouped keys if no prefix given and keys match before delimeter", function(done){
		bucket.PUTObject("noPrefix/one", "value1", function(){
			bucket.PUTObject("noPrefix/two", "value2", function(){
				bucket.GETBucketListObjects(null, null, "/", 10, function(response){
					expect(response.attrs.common_prefixes.indexOf("noPrefix/")).to.be.above(-1);
					expect(response.fetched["noPrefix/"]).to.be.undefined;
					done();
				});
			});
		});
	});

	it("should return no grouped keys if no delimeter specified in GETBucketListObjects", function(done){
		bucket.GETBucketListObjects("key", null, null, 10, function(response){
			expect(response.attrs.common_prefixes).to.be.undefined;
			done();
		});
	});


	it.skip("should only return keys occurring alphabetically AFTER marker", function(done){
		bucket.GETBucketListObjects("key", "k", "/", 10, function(response){
			// console.log("response", response);
			//Geting odd results from testing marker.  Why does the prefix have to be contained in marker?
			// Couldn't the prefix be "key" and the marker "a", so we would actually pick up everything? 
			//From the AWS docs, there is nothing saying that the marker necessarily limits the results.
			done();
		});
	});

	it.skip("should return a next_marker if maxKeys reached", function(done){
		bucket.GETBucketListObjects(null, null, null, 3, function(response){
			// console.log("response", response);
			//Not returning a next_marker when no common prefixes.  
			//Need to complete test.  
			done();
		});
	});
	
 
});








