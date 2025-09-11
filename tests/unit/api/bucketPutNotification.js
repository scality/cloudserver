const assert = require('assert');
const { errors } = require('arsenal');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutNotification = require('../../../lib/api/bucketPutNotification');
const { cleanup,
    DummyRequestLogger,
    makeAuthInfo }
    = require('../helpers');
const metadata = require('../../../lib/metadata/wrapper');

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'bucketname';
const bucketPutRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
    actionImplicitDenies: false,
};

const expectedNotifConfig = {
    queueConfig: [
        {
            id: 'notification-id',
            events: [
                's3:ObjectCreated:*',
                's3:ObjectTagging:*',
                's3:ObjectAcl:Put',
            ],
            queueArn: 'arn:scality:bucketnotif:::target1',
            filterRules: undefined,
        },
    ],
};

function getNotifRequest(empty) {
    const queueConfig = empty ? '' :
        '<QueueConfiguration>' +
        '<Id>notification-id</Id>' +
        '<Queue>arn:scality:bucketnotif:::target1</Queue>' +
        '<Event>s3:ObjectCreated:*</Event>' +
        '<Event>s3:ObjectTagging:*</Event>' +
        '<Event>s3:ObjectAcl:Put</Event>' +
        '</QueueConfiguration>';

    const notifXml = '<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
        `${queueConfig}` +
        '</NotificationConfiguration>';

    const putNotifConfigRequest = {
        bucketName,
        headers: {
            host: `${bucketName}.s3.amazonaws.com`,
        },
        post: notifXml,
        actionImplicitDenies: false,
    };
    return putNotifConfigRequest;
}

describe('putBucketNotification API', () => {
    before(cleanup);
    beforeEach(done => bucketPut(authInfo, bucketPutRequest, log, done));
    afterEach(cleanup);

    it('should update bucket metadata with bucket notification obj', done => {
        bucketPutNotification(authInfo, getNotifRequest(), log, err => {
            assert.ifError(err);
            return metadata.getBucket(bucketName, log, (err, bucket) => {
                assert.ifError(err);
                const bucketNotifConfig = bucket.getNotificationConfiguration();
                assert.deepStrictEqual(bucketNotifConfig, expectedNotifConfig);
                done();
            });
        });
    });

    it('should update bucket metadata with empty bucket notification', done => {
        bucketPutNotification(authInfo, getNotifRequest(true), log, err => {
            assert.ifError(err);
            return metadata.getBucket(bucketName, log, (err, bucket) => {
                assert.ifError(err);
                const bucketNotifConfig = bucket.getNotificationConfiguration();
                assert.deepStrictEqual(bucketNotifConfig, {});
                done();
            });
        });
    });

    describe('checksum validation', () => {
        const notificationXml = '<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
            '<QueueConfiguration>' +
            '<Id>test-notification</Id>' +
            '<Queue>arn:scality:bucketnotif:::target1</Queue>' +
            '<Event>s3:ObjectCreated:Put</Event>' +
            '</QueueConfiguration>' +
            '</NotificationConfiguration>';

        it('should not return an error when Content-MD5 header is missing', done => {
            const testNotificationRequest = {
                bucketName,
                headers: { host: `${bucketName}.s3.amazonaws.com` },
                post: notificationXml,
                url: '/?notification',
                query: { notification: '' },
                actionImplicitDenies: false,
            };

            bucketPutNotification(authInfo, testNotificationRequest, log, err => {
                assert.ifError(err);
                done();
            });
        });

        it('should return BadDigest error when Content-MD5 header mismatches', done => {
            const testNotificationRequest = {
                bucketName,
                headers: {
                    'host': `${bucketName}.s3.amazonaws.com`,
                    'content-md5': '+5yj3kZsXledyKr18eaUDg==', // incorrect MD5
                },
                post: notificationXml,
                url: '/?notification',
                query: { notification: '' },
                actionImplicitDenies: false,
            };

            bucketPutNotification(authInfo, testNotificationRequest, log, err => {
                assert.deepStrictEqual(err, errors.BadDigest);
                done();
            });
        });

        it('should not return an error when Content-MD5 header matches', done => {
            const testNotificationRequest = {
                bucketName,
                headers: {
                    'host': `${bucketName}.s3.amazonaws.com`,
                    'content-md5': '7GuskjLog88KRxugTVDPWg==', // correct MD5
                },
                post: notificationXml,
                url: '/?notification',
                query: { notification: '' },
                actionImplicitDenies: false,
            };

            bucketPutNotification(authInfo, testNotificationRequest, log, err => {
                assert.ifError(err);
                done();
            });
        });
    });
});
