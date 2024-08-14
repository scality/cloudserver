const assert = require('assert');
const sinon = require('sinon');
const async = require('async');
const { PassThrough } = require('stream');
const { errors, versioning } = require('arsenal');
const objectPost = require('../../../lib/api/objectPost');
const {
    getObjectSSEConfiguration
} = require('../../../lib/api/apiUtils/bucket/bucketEncryption.js'); // Update the path as necessary
const collectCorsHeaders = require('../../../lib/utilities/collectCorsHeaders.js'); // Update the path as necessary
const createAndStoreObject = require('../../../lib/api/apiUtils/object/createAndStoreObject.js'); // Update the path as necessary
const metadataUtils = require('../../../lib/metadata/metadataUtils.js'); // Update the path as necessary
const kms = require('../../../lib/kms/wrapper');
const { setExpirationHeaders } = require('../../../lib/api/apiUtils/object/expirationHeaders.js'); // Update the path as necessary
const { pushMetric } = require('../../../lib/utapi/utilities.js'); // Update the path as necessary
const { validateHeaders } = require('../../../lib/api/apiUtils/object/objectLockHelpers.js'); // Update the path as necessary
const writeContinue = require('../../../lib/utilities/writeContinue.js'); // Update the path as necessary
const { debug } = require('console');

describe('objectPost', () => {
    let log, callback, request, authInfo;

    beforeEach(() => {
        log = {
            trace: sinon.stub(),
            error: sinon.stub(),
            debug: sinon.stub(),
        };
        callback = sinon.stub();
        request = {
            headers: {},
            method: 'POST',
            formData: {
                bucket: 'test-bucket',
                key: 'test-key'
            },
            file: new PassThrough()
        };
        authInfo = {
            getCanonicalID: sinon.stub().returns('canonicalID')
        };
    });

    afterEach(() => {
        sinon.restore();
    });

    it('should return NoSuchBucket error if bucket does not exist', (done) => {
        sinon.stub(metadataUtils, 'standardMetadataValidateBucketAndObj').callsFake((params, actionImplicitDenies, log, callback) => {
            callback(errors.NoSuchBucket);
        });

        objectPost(authInfo, request, null, log, callback);

        process.nextTick(() => {
            assert(callback.calledOnce);
            assert(callback.calledWith(errors.NoSuchBucket));
            done();
        });
    });

    it('should return AccessDenied error if user is not authorized', (done) => {
        sinon.stub(metadataUtils, 'standardMetadataValidateBucketAndObj').callsFake((params, actionImplicitDenies, log, callback) => {
            const err = new Error('AccessDenied');
            err.AccessDenied = true;
            callback(err);
        });

        objectPost(authInfo, request, null, log, callback);

        process.nextTick(() => {
            assert(callback.calledOnce);
            assert(callback.calledWithMatch(sinon.match.has('AccessDenied')));
            done();
        });
    });

    it('should successfully post an object', (done) => {
        const bucket = {
            getOwner: sinon.stub().returns('ownerID'),
            hasDeletedFlag: sinon.stub().returns(false),
            getLifecycleConfiguration: sinon.stub().returns(null),
            getVersioningConfiguration: sinon.stub().returns({ Status: 'Enabled' }),
            getLocationConstraint: sinon.stub().returns('location')
        };

        const objMD = {};
        const responseHeaders = {};

        sinon.stub(metadataUtils, 'standardMetadataValidateBucketAndObj').callsFake((params, actionImplicitDenies, log, callback) => {
            callback(null, bucket, objMD);
        });

        sinon.stub(collectCorsHeaders, 'collectCorsHeaders').returns(responseHeaders);
        sinon.stub(getObjectSSEConfiguration, 'getObjectSSEConfiguration').callsFake((headers, bucket, log, callback) => {
            callback(null, null);
        });
        sinon.stub(kms, 'createCipherBundle').callsFake((serverSideEncryptionConfig, log, callback) => {
            callback(null, null);
        });
        sinon.stub(validateHeaders, 'validateHeaders').returns(null);
        sinon.stub(writeContinue, 'writeContinue').returns(null);
        sinon.stub(createAndStoreObject, 'createAndStoreObject').callsFake((bucketName, bucket, key, objMD, authInfo, canonicalID, cipherBundle, request, isDeleteMarker, streamingV4Params, overheadField, log, callback) => {
            callback(null, { contentMD5: 'md5', lastModified: new Date(), versionId: 'versionId' });
        });
        sinon.stub(setExpirationHeaders, 'setExpirationHeaders').returns(null);
        sinon.stub(pushMetric, 'pushMetric').returns(null);

        request.file.end('filecontent');

        objectPost(authInfo, request, null, log, callback);

        process.nextTick(() => {
            assert(callback.calledOnce);
            assert(callback.calledWith(null, responseHeaders));
            done();
        });
    });
});
