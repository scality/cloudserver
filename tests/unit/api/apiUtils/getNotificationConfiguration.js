const assert = require('assert');
const sinon = require('sinon');
const { config } = require('../../../../lib/Config');
const errors = require('arsenal').errors;

const getNotificationConfiguration =
    require('../../../../lib/api/apiUtils/bucket/getNotificationConfiguration');

const parsedXml = {
    NotificationConfiguration: {
        QueueConfiguration: [
            {
                Id: ['notification-id'],
                Event: ['s3:ObjectCreated:*'],
                Queue: ['arn:scality:bucketnotif:::target1'],
            },
        ],
    }
};

const expectedConfig = {
    queueConfig: [
        {
          events: ['s3:ObjectCreated:*'],
          queueArn: 'arn:scality:bucketnotif:::target1',
          id: 'notification-id',
          filterRules: undefined
        }
      ]
};

const destination1 = [
    {
        resource: 'target1',
        type: 'dummy',
        host: 'localhost:6000',
    }
];

const destinations2 = [
    {
        resource: 'target2',
        type: 'dummy',
        host: 'localhost:6000',
    }
];

describe('getNotificationConfiguration', () => {
    afterEach(() => sinon.restore());

    it('should return notification configuration', done => {
        sinon.stub(config, 'bucketNotificationDestinations').value(destination1);
        const notifConfig = getNotificationConfiguration(parsedXml);
        assert.deepEqual(notifConfig, expectedConfig);
        return done();
    });

    it('should return empty notification configuration', done => {
        sinon.stub(config, 'bucketNotificationDestinations').value(destination1);
        const notifConfig = getNotificationConfiguration({
            NotificationConfiguration: {}
        });
        assert.deepEqual(notifConfig, {});
        return done();
    });

    it('should return error if no destinations found', done => {
        sinon.stub(config, 'bucketNotificationDestinations').value([]);
        const notifConfig = getNotificationConfiguration(parsedXml);
        assert.deepEqual(notifConfig.error, errors.InvalidArgument);
        return done();
    });

    it('should return error if destination invalid', done => {
        sinon.stub(config, 'bucketNotificationDestinations').value(destinations2);
        const notifConfig = getNotificationConfiguration(parsedXml);
        assert.deepEqual(notifConfig.error, errors.InvalidArgument);
        const invalidArguments = notifConfig.error.metadata.get('invalidArguments');
        assert.deepEqual(invalidArguments, [{
            ArgumentName: 'arn:scality:bucketnotif:::target1',
            ArgumentValue: 'The destination queue does not exist',
        }]);
        return done();
    });
});
