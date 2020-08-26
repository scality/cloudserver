const assert = require('assert');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketGetNotification = require('../../../lib/api/bucketGetNotification');
const bucketPutNotification = require('../../../lib/api/bucketPutNotification');
const { cleanup,
    DummyRequestLogger,
    makeAuthInfo }
    = require('../helpers');

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'bucketname';
const testBucketPutRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
};

function getNotificationRequest(bucketName, xml) {
    const request = {
        bucketName,
        headers: {
            host: `${bucketName}.s3.amazonaws.com`,
        },
    };
    if (xml) {
        request.post = xml;
    }
    return request;
}

function getNotificationXml() {
    const id = 'queue1';
    const id2 = 'queue2';
    const event = 's3:ObjectCreated:Put';
    const event2 = 's3:ObjectCreated:CompleteMultipartUpload';
    const event3 = 's3:ObjectRemoved:Delete';
    const queueArn = 'arn:scality:bucketnotif:::target1';
    const queueArn2 = 'arn:scality:bucketnotif:::target2';
    const filterName = 'prefix';
    const filterValue = 'logs/'

    return '<NotificationConfiguration>' +
        '<QueueConfiguration>' +
        `<Id>${id}</Id>` +
        `<Event>${event}</Event>` +
        `<Event>${event2}</Event>` +
        `<QueueArn>${queueArn}</QueueArn>` +
        '<Filter><S3Key>' +
        `<FilterRule><Name>${filterName}</Name>` +
        `<Value>${filterValue}</Value></FilterRule>` +
        '<S3Key><Filter>' +
        '</QueueConfiguration>' +
        '<QueueConfiguration>' +
        `<Id>${id2}</Id>` +
        `<Event>${event3}</Event>` +
        `<QueueArn>${queueArn2}</QueueArn>` +
        '</QueueConfiguration>' +
        '</NotificationConfiguration>';
}


describe('getBucketNotification API', () => {
    before(() => cleanup());
    beforeEach(done => bucketPut(authInfo, testBucketPutRequest, log, done));
    afterEach(() => cleanup());

    it('should not return an error if bucket has no notification config', done => {
        const notificationRequest = getNotificationRequest(bucketName);
        bucketGetNotification(authInfo, notificationRequest, log, err => {
            assert.ifError(err);
            done();
        });
    });

    describe('after bucket notification has been put', () => {
        beforeEach(done => {
            const putRequest =
                getNotificationRequest(bucketName, getNotificationXml());
            bucketPutNotification(authInfo, putRequest, log, err => {
                assert.equal(err, null);
                done();
            });
        });

        it('should return notification XML', done => {
            const getRequest = getNotificationRequest(bucketName);
            bucketGetNotification(authInfo, getRequest, log, (err, res) => {
                assert.equal(err, null);
                const expectedXML = '<?xml version="1.0" encoding="UTF-8"?>' +
                    `${getNotificationXml()}`;
                assert.deepStrictEqual(expectedXML, res);
                done();
            });
        });
    });
});
