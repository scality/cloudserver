import assert from 'assert';

import bucketPut from '../../../lib/api/bucketPut';
import bucketGetLocation from '../../../lib/api/bucketGetLocation';
import { cleanup,
    DummyRequestLogger,
    makeAuthInfo }
from '../helpers';
import config from '../../../lib/Config';

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'bucketGetLocationTestBucket';

const testBucketPutRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
};

const testGetLocationRequest = {
    bucketName,
    headers: {
        host: `${bucketName}.s3.amazonaws.com`,
    },
    url: '/?location',
    query: { location: '' },
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
