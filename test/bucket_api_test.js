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
		1) 	Return individual key if key does not contain the delimiter (even if key begins with specified prefix).
		2)	Return key under common prefix if key begins with prefix and contains delimiter.  All 
				keys that contain the same substring starting with the prefix and ending with the first 
				occurrence of the delimiter will be grouped together and appear once under common prefix.  
				For instance, "key2/sample" and "key2/moreSample" will be 
				grouped together if prefix is "key" and delimiter is "/".  
		3)	If do not specify prefix, return grouped keys under common prefix if they contain 
				same substring starting at beginning of the key and ending before first occurrence of delimiter.
		4)	There will be no grouping if no delimiter specified as argument in GETBucketListObjects.		
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

	it("should return individual key if key does not contain the delimiter even if key contains prefix", function(done){
		bucket.PUTObject("key1", "valueWithoutDelimiter", function(){
			bucket.PUTObject("noMatchKey", "non-matching key", function(){
				bucket.PUTObject("key1/", "valueWithDelimiter", function(){
					bucket.GETBucketListObjects("key", null, "/", 10, function(response){
						expect(response.fetched["key1"]).to.equal("valueWithoutDelimiter");
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


	it("should return grouped keys under common prefix if keys start with given prefix and contain given delimiter", function(done){
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

	it("should return grouped keys if no prefix given and keys match before delimiter", function(done){
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

	it("should return no grouped keys if no delimiter specified in GETBucketListObjects", function(done){
		bucket.GETBucketListObjects("key", null, null, 10, function(response){
			expect(response.attrs.common_prefixes).to.be.undefined;
			done();
		});
	});

	it("should only return keys occurring alphabetically AFTER marker when no delimiter specified", function(done){
		//This test is currently failing but based on my tests on AWS command line, this test should pass. 
		 bucket.PUTObject("a", "shouldBeExcluded", function(){
			bucket.PUTObject("b", "shouldBeIncluded", function(){
				bucket.GETBucketListObjects(null, "a", null, 10, function(response){
					expect(response.fetched["b"]).to.equal("shouldBeIncluded");
					expect(response.fetched["a"]).to.be.undefined;
					done();
				});
			});
		});
	});


	it("should only return keys occurring alphabetically AFTER marker when delimiter specified", function(done){
		//This test is currently failing but based on my tests on AWS command line, this test should pass. 
		bucket.GETBucketListObjects(null, "a", "/", 10, function(response){
			expect(response.fetched["b"]).to.equal("shouldBeIncluded");
			expect(response.fetched["a"]).to.be.undefined;
			done();
		});
	});

	it("should only return keys occurring alphabetically AFTER marker when delimiter and prefix specified", function(done){
		//This test is currently failing but based on my tests on AWS command line, this test should pass. 
		bucket.GETBucketListObjects("b", "a", "/", 10, function(response){
			expect(response.fetched["b"]).to.equal("shouldBeIncluded");
			expect(response.fetched["a"]).to.be.undefined;
			done();
		});
	});

	it("should return a next_marker if maxKeys reached", function(done){
		//This test is currently failing but based on my tests on AWS command line, this test should pass. 
		 bucket.PUTObject("next/", "shouldBeListed", function(){
		 	bucket.PUTObject("next/rollUp", "shouldBeRolledUp", function(){
				bucket.PUTObject("next1/", "shouldBeNextMarker", function(){
					bucket.GETBucketListObjects("next", null, "/", 1, function(response){
						expect(response.attrs.common_prefixes.indexOf("next/")).to.be.above(-1);
						expect(response.attrs.common_prefixes.indexOf("next1/")).to.equal(-1);
						expect(response.attrs.next_marker).to.equal("next1/");
						expect(response.attrs.truncated).to.be.true;
						done();
					});
				});
			});
		});
	});


});


describe("stress test for bucket API", function(){

	this.timeout(200000);
	var makeid = require("./makeid.js");
	var async = require("async");
	//Test should be of at least 100,000 keys
	var NUM_KEYS = 100000;
	//We expect 1,000 puts per second
	var MAX_MILLISECONDS = NUM_KEYS;
	var bucket;
			
	before(function() {
	  bucket = new Bucket();
	});

	after(function(done) {
		bucket.DELETEBucket(function(){
			done();
		});
	});

	it("should put NUM_KEYS keys into bucket and retrieve full list in under MAX_MILLISECONDS", function(done){
    var delimiter = "/";
		var data = {};
		var keys = [];
		
		var prefixes = ["dogs","cats"];

		//Create dictionary entries based on prefixes array
		for(var i=0; i< prefixes.length; i++){
			data[prefixes[i]] = [];
		}

		//Populate dictionary with random key extensions
		for (var j=0; j<NUM_KEYS; j++){
			var prefix = prefixes[j % prefixes.length];
			data[prefix].push(makeid(10));
		};

		//Populate keys array with all keys including prefixes
		for(var key in data){
			for(var k=0; k< data[key].length; k++){
				keys.push(key + "/" + data[key][k]);
			};
		};

		//randomize it

		//Start timing
		var time = process.hrtime();

		async.each(keys, function(item, next){
			bucket.PUTObject(item, "value", next);
		}, function(err){
			if(err){
				console.error('Error' + err);
				expect(err).to.be.undefined;
				done();
			} else {
				bucket.GETBucketListObjects(null, null, '/', 1000, function(response){
				console.log("response", response);
				//Stop timing
				var diff = process.hrtime(time);
				//var diff = run of function with hrtime return milliseconds.  
//make funcyion to convert diff into milliseconds.  
				var seconds = diff[0] * 1000 + (diff[1]/1e9);
				console.log("seconds", seconds)
				expect(seconds).to.be.below(MAX_MILLISECONDS);
				expect(response.attrs.common_prefixes.indexOf("dogs/")).to.be.above(-1);
				expect(response.attrs.common_prefixes.indexOf("cats/")).to.be.above(-1);
				done();
				});
			};
		});
	});

});








