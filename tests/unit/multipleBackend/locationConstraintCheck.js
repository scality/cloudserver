const assert = require('assert');
const { BackendInfo } = require('../../../lib/api/apiUtils/object/BackendInfo');
const BucketInfo = require('../../../lib/metadata/BucketInfo');
const DummyRequest = require('../DummyRequest');
const { DummyRequestLogger } = require('../helpers');
const locationConstraintCheck
    = require('../../../lib/api/apiUtils/object/locationConstraintCheck');

const bucketName = 'nameOfBucket';
const owner = 'canonicalID';
const ownerDisplayName = 'bucketOwner';
const testDate = new Date().toJSON();
const locationConstraint = 'file';
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
    it('should return error if controlling location constraint is ' +
    'not valid', done => {
        const backendInfoObj = locationConstraintCheck(
            createTestRequest('fail-region'), null, testBucket, log);
        assert.strictEqual(backendInfoObj.err.code, 400,
            'Expected "Invalid Argument" code error');
        assert(backendInfoObj.err.InvalidArgument, 'Expected "Invalid ' +
        'Argument" error');
        done();
    });

    it('should return instance of BackendInfo with correct ' +
    'locationConstraints', done => {
        const backendInfoObj = locationConstraintCheck(
            createTestRequest('mem'), null, testBucket, log);
        assert.strictEqual(backendInfoObj.err, null, 'Expected success ' +
            `but got error ${backendInfoObj.err}`);
        assert.strictEqual(typeof backendInfoObj.controllingLC, 'string');
        assert.equal(backendInfoObj.backendInfo instanceof BackendInfo,
            true);
        assert.strictEqual(backendInfoObj.
            backendInfo.getObjectLocationConstraint(), 'mem');
        assert.strictEqual(backendInfoObj.
            backendInfo.getBucketLocationConstraint(), 'file');
        assert.strictEqual(backendInfoObj.backendInfo.getRequestEndpoint(),
            'localhost');
        done();
    });
});
