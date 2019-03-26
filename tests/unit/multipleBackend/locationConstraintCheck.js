const assert = require('assert');

const { BucketInfo, BackendInfo } = require('arsenal').models;
const DummyRequest = require('../DummyRequest');
const { DummyRequestLogger } = require('../helpers');
const locationConstraintCheck
    = require('../../../lib/api/apiUtils/object/locationConstraintCheck');

const memLocation = 'scality-internal-mem';
const fileLocation = 'scality-internal-file';
const bucketName = 'nameOfBucket';
const owner = 'canonicalID';
const ownerDisplayName = 'bucketOwner';
const testDate = new Date().toJSON();
const locationConstraint = fileLocation;
const namespace = 'default';
const objectKey = 'someobject';
const postBody = Buffer.from('I am a body', 'utf8');

const log = new DummyRequestLogger();
const testBucket = new BucketInfo(bucketName, owner, ownerDisplayName,
    testDate, null, null, null, null, null, null, locationConstraint);

function createTestRequest(locationConstraint) {
    const testRequest = new DummyRequest({
        bucketName,
        namespace,
        objectKey,
        headers: { 'x-amz-meta-scal-location-constraint': locationConstraint },
        url: `/${bucketName}/${objectKey}`,
        parsedHost: 'localhost',
    }, postBody);
    return testRequest;
}

describe('Location Constraint Check', () => {
    test('should return error if controlling location constraint is ' +
    'not valid', done => {
        const backendInfoObj = locationConstraintCheck(
            createTestRequest('fail-region'), null, testBucket, log);
        expect(backendInfoObj.err.code).toBe(400);
        expect(backendInfoObj.err.InvalidArgument).toBeTruthy();
        done();
    });

    test('should return instance of BackendInfo with correct ' +
    'locationConstraints', done => {
        const backendInfoObj = locationConstraintCheck(
            createTestRequest(memLocation), null, testBucket, log);
        expect(backendInfoObj.err).toBe(null);
        expect(typeof backendInfoObj.controllingLC).toBe('string');
        expect(backendInfoObj.backendInfo instanceof BackendInfo).toEqual(true);
        expect(backendInfoObj.
            backendInfo.getObjectLocationConstraint()).toBe(memLocation);
        expect(backendInfoObj.
            backendInfo.getBucketLocationConstraint()).toBe(fileLocation);
        expect(backendInfoObj.backendInfo.getRequestEndpoint()).toBe('localhost');
        done();
    });
});
