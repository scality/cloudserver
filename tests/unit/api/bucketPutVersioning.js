const assert = require('assert');
const async = require('async');

const { errors } = require('arsenal');
const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutVersioning = require('../../../lib/api/bucketPutVersioning');
const bucketPutReplication = require('../../../lib/api/bucketPutReplication');

const { cleanup,
    DummyRequestLogger,
    makeAuthInfo } = require('../helpers');
const metadata = require('../../../lib/metadata/wrapper');

const xmlEnableVersioning =
'<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
'<Status>Enabled</Status>' +
'</VersioningConfiguration>';

const xmlSuspendVersioning =
'<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
'<Status>Suspended</Status>' +
'</VersioningConfiguration>';

const locConstraintVersioned =
'<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
'<LocationConstraint>withversioning</LocationConstraint>' +
'</CreateBucketConfiguration>';

const locConstraintNonVersioned =
'<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
'<LocationConstraint>withoutversioning</LocationConstraint>' +
'</CreateBucketConfiguration>';

const xmlReplicationConfiguration =
'<ReplicationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
    '<Role>arn:aws:iam::account-id:role/src-resource</Role>' +
    '<Rule>' +
        '<Prefix></Prefix>' +
        '<Status>Enabled</Status>' +
        '<Destination>' +
            '<Bucket>arn:aws:s3:::destination-bucket</Bucket>' +
            '<StorageClass>us-east-2</StorageClass>' +
        '</Destination>' +
    '</Rule>' +
'</ReplicationConfiguration>';

const externalVersioningErrorMessage = 'We do not currently support putting ' +
'a versioned object to a location-constraint of type Azure or GCP.';

const log = new DummyRequestLogger();
const bucketName = 'bucketname';
const authInfo = makeAuthInfo('accessKey1');

function _getPutBucketRequest(xml) {
    const request = {
        bucketName,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: '/',
    };
    request.post = xml;
    return request;
}

function _putReplicationRequest(xml) {
    const request = {
        bucketName,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: '/?replication',
    };
    request.post = xml;
    return request;
}

function _putVersioningRequest(xml) {
    const request = {
        bucketName,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        url: '/?versioning',
        query: { versioning: '' },
    };
    request.post = xml;
    return request;
}

describe('bucketPutVersioning API', () => {
    before(() => cleanup());
    afterEach(() => cleanup());

    describe('with version enabled location constraint', () => {
        beforeEach(done => {
            const request = _getPutBucketRequest(locConstraintVersioned);
            bucketPut(authInfo, request, log, done);
        });

        const tests = [
            {
                msg: 'should successfully enable versioning on location ' +
                'constraint with supportsVersioning set to true',
                input: xmlEnableVersioning,
                output: { Status: 'Enabled' },
            },
            {
                msg: 'should successfully suspend versioning on location ' +
                'constraint with supportsVersioning set to true',
                input: xmlSuspendVersioning,
                output: { Status: 'Suspended' },
            },
        ];
        tests.forEach(test => it(test.msg, done => {
            const request = _putVersioningRequest(test.input);
            bucketPutVersioning(authInfo, request, log, err => {
                assert.ifError(err,
                    `Expected success, but got err: ${err}`);
                metadata.getBucket(bucketName, log, (err, bucket) => {
                    assert.ifError(err,
                        `Expected success, but got err: ${err}`);
                    assert.deepStrictEqual(bucket._versioningConfiguration,
                        test.output);
                    done();
                });
            });
        }));

        it('should not suspend versioning on bucket with replication', done => {
            async.series([
                // Enable versioning to allow putting a replication config.
                next => {
                    const request = _putVersioningRequest(xmlEnableVersioning);
                    bucketPutVersioning(authInfo, request, log, next);
                },
                // Put the replication config on the bucket.
                next => {
                    const request =
                        _putReplicationRequest(xmlReplicationConfiguration);
                    bucketPutReplication(authInfo, request, log, next);
                },
                // Attempt to suspend versioning.
                next => {
                    const request = _putVersioningRequest(xmlSuspendVersioning);
                    bucketPutVersioning(authInfo, request, log, err => {
                        assert(err.InvalidBucketState);
                        next();
                    });
                },
            ], done);
        });
    });

    describe('with version disabled location constraint', () => {
        beforeEach(done => {
            const request = _getPutBucketRequest(locConstraintNonVersioned);
            bucketPut(authInfo, request, log, done);
        });

        const tests = [
            {
                msg: 'should return error if enabling versioning on location ' +
                'constraint with supportsVersioning set to false',
                input: xmlEnableVersioning,
                output: { error: errors.NotImplemented.customizeDescription(
                    externalVersioningErrorMessage) },
            },
            {
                msg: 'should return error if suspending versioning on ' +
                ' location constraint with supportsVersioning set to false',
                input: xmlSuspendVersioning,
                output: { error: errors.NotImplemented.customizeDescription(
                    externalVersioningErrorMessage) },
            },
        ];
        tests.forEach(test => it(test.msg, done => {
            const putBucketVersioningRequest =
                _putVersioningRequest(test.input);
            bucketPutVersioning(authInfo, putBucketVersioningRequest, log,
            err => {
                assert.deepStrictEqual(err, test.output.error);
                done();
            });
        }));
    });
});
