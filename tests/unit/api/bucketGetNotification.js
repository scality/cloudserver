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
    const event = 's3:ObjectCreated:Put';
    const event2 = 's3:ObjectCreated:CompleteMultipartUpload';
    const queueArn = 'arn:scality:bucketnotif:::target1';
    const filterName = 'Prefix';
    const filterValue = 'logs/';

    return '<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
        '<QueueConfiguration>' +
        `<Id>${id}</Id>` +
        `<Queue>${queueArn}</Queue>` +
        `<Event>${event}</Event>` +
        `<Event>${event2}</Event>` +
        '<Filter><S3Key>' +
        `<FilterRule><Name>${filterName}</Name>` +
        `<Value>${filterValue}</Value></FilterRule>` +
        '</S3Key></Filter>' +
        '</QueueConfiguration>' +
        '</NotificationConfiguration>';
}


describe('getBucketNotification API', () => {
    before(cleanup);
    beforeEach(done => bucketPut(authInfo, testBucketPutRequest, log, done));
    afterEach(cleanup);

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
                assert.ifError(err);
                done();
            });
        });

        it('should return notification XML', done => {
            const getRequest = getNotificationRequest(bucketName);
            bucketGetNotification(authInfo, getRequest, log, (err, res) => {
                assert.ifError(err);
                const expectedXML = '<?xml version="1.0" encoding="UTF-8"?>' +
                    `${getNotificationXml()}`;
                assert.deepStrictEqual(expectedXML, res);
                done();
            });
        });
    });
});
