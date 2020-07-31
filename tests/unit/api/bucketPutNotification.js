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
            filterRules: [],
        },
    ],
};

const notifXml = '<NotificationConfiguration>' +
    '<QueueConfiguration>' +
    '<Id>notification-id</Id>' +
    '<Queue>arn:scality:bucketnotif:::target1</Queue>' +
    '<Event>s3:ObjectCreated:*</Event>' +
    '</QueueConfiguration>' +
    '</NotificationConfiguration>';

const putNotifConfigRequest = {
    bucketName,
    headers: {
        host: `${bucketName}.s3.amazonaws.com`,
    },
    post: notifXml,
};

describe('putBucketNotification API', () => {
    before(() => cleanup());
    beforeEach(done => bucketPut(authInfo, bucketPutRequest, log, done));
    afterEach(() => cleanup());

    it('should update a bucket\'s metadata with bucket notification obj',
    done => {
        bucketPutNotification(authInfo, putNotifConfigRequest, log, err => {
            if (err) {
                process.stdout.write(`Err putting bucket notification ${err}`);
                return done(err);
            }
            return metadata.getBucket(bucketName, log, (err, bucket) => {
                if (err) {
                    process.stdout.write(`Err retrieving bucket MD ${err}`);
                    return done(err);
                }
                const bucketNotifConfig = bucket.getNotificationConfiguration();
                assert.deepStrictEqual(bucketNotifConfig, expectedNotifConfig);
                return done();
            });
        });
    });
});
