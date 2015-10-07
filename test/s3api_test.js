'use strict';

const chai = require("chai");
const expect = chai.expect;
const utils = require('../lib/utils.js');
const bucketPut = require('../lib/api/bucketPut.js');
// const Bucket = require("../lib/bucket_mem.js");
// const utilities = require("../lib/bucket_utilities.js");

describe("bucketPut API",function(){

	const accessKey = 'accessKey1';
	const metastore = require('../lib/testdata/metadata.json');	

	it("should return an error if no bucketname provided", function(done){

		const testRequest = {
			lowerCaseHeaders:
			   { host: '127.0.0.1:8000',
			     'accept-encoding': 'identity',
			     'content-length': '0',
			     authorization: 'AWS accessKey1:DOiE48Tln2KxFIOWi0iafB7XG90=',
			     'x-amz-date': 'Wed, 07 Oct 2015 17:38:31 +0000' },
			 url: '/',
			 namespace: 'default',
			 post: ''
		}

		bucketPut(accessKey, metastore, testRequest, function(err, result) {
			expect(err).to.equal('Missing bucket name');
			done();
		})

	});

	it("should return an error if bucketname is invalid", function(done){

		const testRequest = {
			lowerCaseHeaders:
			   { host: '127.0.0.1:8000',
			     'accept-encoding': 'identity',
			     'content-length': '0',
			     authorization: 'AWS accessKey1:DOiE48Tln2KxFIOWi0iafB7XG90=',
			     'x-amz-date': 'Wed, 07 Oct 2015 17:38:31 +0000' },
			 url: '/hi',
			 namespace: 'default',
			 post: ''
		}

		bucketPut(accessKey, metastore, testRequest, function(err, result) {
			expect(err).to.equal('Bucket name is invalid');
			done();
		})

	});

	it("should return an error if improper xml is provided in request.post", function(done){

		const testRequest = {
			lowerCaseHeaders:
			   { host: '127.0.0.1:8000',
			     'accept-encoding': 'identity',
			     'content-length': '0',
			     authorization: 'AWS accessKey1:DOiE48Tln2KxFIOWi0iafB7XG90=',
			     'x-amz-date': 'Wed, 07 Oct 2015 17:38:31 +0000' },
			 url: '/test1',
			 namespace: 'default',
			 post: 'improperxml'
		}

		bucketPut(accessKey, metastore, testRequest, function(err, result) {
			expect(err).to.equal('Improper XML');
			done();
		})

	});


	it("should return an error if xml which does not conform to s3 docs is provided in request.post", function(done){

		const testRequest = {
			lowerCaseHeaders:
			   { host: '127.0.0.1:8000',
			     'accept-encoding': 'identity',
			     'content-length': '0',
			     authorization: 'AWS accessKey1:DOiE48Tln2KxFIOWi0iafB7XG90=',
			     'x-amz-date': 'Wed, 07 Oct 2015 17:38:31 +0000' },
			 url: '/test1',
			 namespace: 'default',
			 post: '<Hello></Hello>'
		}

		bucketPut(accessKey, metastore, testRequest, function(err, result) {
			expect(err).to.equal('LocationConstraint improperly specified');
			done();
		})

	});


	it("should create a bucket using bucket name provided in path", function(done){

		const bucketName = 'test1'
		const testRequest = {
			lowerCaseHeaders:
			   { host: '127.0.0.1:8000',
			     'accept-encoding': 'identity',
			     'content-length': '0',
			     authorization: 'AWS accessKey1:DOiE48Tln2KxFIOWi0iafB7XG90=',
			     'x-amz-date': 'Wed, 07 Oct 2015 17:38:31 +0000' },
			 url: `/${bucketName}`,
			 namespace: 'default',
			 post: ''
		}

		const testBucketUID = utils.getResourceUID(testRequest.namespace, bucketName);

		bucketPut(accessKey, metastore, testRequest, function(err, success) {
			expect(success).to.equal('Bucket created');
			expect(metastore.buckets[testBucketUID].name).to.equal(bucketName);
			expect(metastore.buckets[testBucketUID].owner).to.equal(accessKey);
			expect(metastore.users[accessKey].buckets).to.have.length.of.at.least(1);
			done();
		})

	});


	it.only("should create a bucket using bucket name provided in host", function(done){

		const bucketName = 'BucketName'
		const testRequest = {
			lowerCaseHeaders:
			   { host: `${bucketName}.s3.amazonaws.com`,
			     'accept-encoding': 'identity',
			     'content-length': '0',
			     authorization: 'AWS accessKey1:DOiE48Tln2KxFIOWi0iafB7XG90=',
			     'x-amz-date': 'Wed, 07 Oct 2015 17:38:31 +0000' },
			 url: '/',
			 namespace: 'default',
			 post: '',
			 headers: {host: `${bucketName}.s3.amazonaws.com`}
		}

		const testBucketUID = utils.getResourceUID(testRequest.namespace, bucketName);

		bucketPut(accessKey, metastore, testRequest, function(err, success) {
			console.log("err", err)
			expect(success).to.equal('Bucket created');
			expect(metastore.buckets[testBucketUID].name).to.equal(bucketName);
			expect(metastore.buckets[testBucketUID].owner).to.equal(accessKey);
			expect(metastore.users[accessKey].buckets).to.have.length.of.at.least(1);
			done();
		})

	});
	


});