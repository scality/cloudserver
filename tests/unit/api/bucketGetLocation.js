const assert = require('assert');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketGetLocation = require('../../../lib/api/bucketGetLocation');
const { cleanup,
    DummyRequestLogger,
    makeAuthInfo }
= require('../helpers');
const { config } = require('../../../lib/Config');

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'bucketGetLocationTestBucket';

const testBucketPutRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
    actionImplicitDenies: false,
};

const testGetLocationRequest = {
    bucketName,
    headers: {
        host: `${bucketName}.s3.amazonaws.com`,
    },
    url: '/?location',
    query: { location: '' },
    actionImplicitDenies: false,
};

const locationConstraints = config.locationConstraints;

function getBucketRequestObject(location) {
    const post = location ? '<?xml version="1.0" encoding="UTF-8"?>' +
        '<CreateBucketConfiguration ' +
        'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
        `<LocationConstraint>${location}</LocationConstraint>` +
        '</CreateBucketConfiguration>' : undefined;
    return Object.assign({ post }, testBucketPutRequest);
}
describe('getBucketLocation API', () => {
    Object.keys(locationConstraints).forEach(location => {
        if (location === 'us-east-1') {
            // if region us-east-1 should return empty string
            // see next test.
            return;
        }
        const bucketPutRequest = getBucketRequestObject(location);
        describe(`with ${location} LocationConstraint`, () => {
            beforeEach(done => {
                cleanup();
                bucketPut(authInfo, bucketPutRequest, log, done);
            });
            afterEach(() => cleanup());
            it(`should return ${location} LocationConstraint xml`, done => {
                bucketGetLocation(authInfo, testGetLocationRequest, log,
                (err, res) => {
                    assert.strictEqual(err, null,
                      `Unexpected ${err} getting location constraint`);
                    const xml = `<?xml version="1.0" encoding="UTF-8"?>
        <LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
          `${location}</LocationConstraint>`;
                    assert.deepStrictEqual(res, xml);
                    return done();
                });
            });
        });
    });
    [undefined, 'us-east-1'].forEach(location => {
        const bucketPutRequest = getBucketRequestObject(location);
        describe(`with ${location} LocationConstraint`, () => {
            beforeEach(done => {
                cleanup();
                bucketPut(authInfo, bucketPutRequest, log, done);
            });
            afterEach(() => cleanup());
            it('should return empty string LocationConstraint xml', done => {
                bucketGetLocation(authInfo, testGetLocationRequest, log,
                (err, res) => {
                    assert.strictEqual(err, null,
                      `Unexpected ${err} getting location constraint`);
                    const xml = `<?xml version="1.0" encoding="UTF-8"?>
        <LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
        '</LocationConstraint>';
                    assert.deepStrictEqual(res, xml);
                    return done();
                });
            });
        });
    });
});
