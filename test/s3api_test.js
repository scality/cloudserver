'use strict';

const chai = require("chai");
const expect = chai.expect;
const utils = require('../lib/utils.js');
const bucketPut = require('../lib/api/bucketPut.js');
const bucketHead = require('../lib/api/bucketHead.js');
const accessKey = 'accessKey1';

describe("bucketPut API",function(){
	let metastore;

	beforeEach(function () {
	   metastore = {
			  "users": {
			      "accessKey1": {
			        "buckets": []
			      },
			      "accessKey2": {
			        "buckets": []
			      }
			  },
			  "buckets": {}
			}
	});


	it("should return an error if no bucketname provided", function(done){

		const testRequest = {
			lowerCaseHeaders: {},
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

		const tooShortBucketName = 'hi';
		const testRequest = {
			lowerCaseHeaders: {},
			 url: `/${tooShortBucketName}`,
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
			lowerCaseHeaders: {},
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
			lowerCaseHeaders: {},
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
			lowerCaseHeaders: {},
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


	it("should create a bucket using bucket name provided in host", function(done){

		const bucketName = 'BucketName'
		const testRequest = {
			lowerCaseHeaders: {},
			 url: '/',
			 namespace: 'default',
			 post: '',
			 headers: {host: `${bucketName}.s3.amazonaws.com`}
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
});


	describe("bucketHead API",function(){

		let metastore;

		beforeEach(function () {
		   metastore = {
				  "users": {
				      "accessKey1": {
				        "buckets": []
				      },
				      "accessKey2": {
				        "buckets": []
				      }
				  },
				  "buckets": {}
				}
		});


		it("should return an error if the bucket does not exist", function(done){
			const bucketName = 'BucketName';
			const testRequest = {
				headers: {host: `${bucketName}.s3.amazonaws.com`},
				url: '/',
				namespace: 'default'
			}

			bucketHead(accessKey, metastore, testRequest, function(err, result) {
				expect(err).to.equal('Bucket does not exist -- 404');
				done();
			})

		});

		it("should return an error if user is not authorized", function(done){
			const bucketName = 'BucketName';
			const putAccessKey = 'accessKey2';
			const testRequest = {
				lowerCaseHeaders: {},
				headers: {host: `${bucketName}.s3.amazonaws.com`},
				url: '/',
				namespace: 'default'
			}

			bucketPut(putAccessKey, metastore, testRequest, function(err, success) {
				expect(success).to.equal('Bucket created');
				bucketHead(accessKey, metastore, testRequest, function(err, result) {
					expect(err).to.equal('Action not permitted -- 403');
					done();
				})
			})
		});

		it("should return a success message if bucket exists and user is authorized", function(done){
			const bucketName = 'BucketName';
			const testRequest = {
				lowerCaseHeaders: {},
				headers: {host: `${bucketName}.s3.amazonaws.com`},
				url: '/',
				namespace: 'default'
			}

			bucketPut(accessKey, metastore, testRequest, function(err, success) {
				expect(success).to.equal('Bucket created');
				bucketHead(accessKey, metastore, testRequest, function(err, result) {
					expect(result).to.equal('Bucket exists and user authorized -- 200');
					done();
				})
			})
		});
});


