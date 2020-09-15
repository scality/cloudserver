const assert = require('assert');

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
};

const expectedNotifConfig = {
    queueConfig: [
        {
            id: 'notification-id',
            events: ['s3:ObjectCreated:*'],
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
});
