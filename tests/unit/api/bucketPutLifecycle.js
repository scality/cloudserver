const assert = require('assert');

const { bucketPut } = require('../../../lib/api/bucketPut');
const bucketPutLifecycle = require('../../../lib/api/bucketPutLifecycle');
const { cleanup,
    DummyRequestLogger,
    makeAuthInfo }
    = require('../helpers');
const metadata = require('../../../lib/metadata/wrapper');

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const bucketName = 'bucketname';
const testBucketPutRequest = {
    bucketName,
    headers: { host: `${bucketName}.s3.amazonaws.com` },
    url: '/',
};

function _getPutLifecycleRequest(xml) {
    const request = {
        bucketName,
        headers: {
            host: `${bucketName}.s3.amazonaws.com`,
        },
    };
    request.post = xml;
    return request;
}

function _getTestLifecycleXml() {
    const id1 = 'test-id1';
    const id2 = 'test-id2';
    const prefix = 'test-prefix';
    const tags = [
        {
            key: 'test-key1',
            value: 'test-value1',
        },
    ];
    const action1 = 'Expiration';
    const days1 = 365;
    const action2 = 'NoncurrentVersionExpiration';
    const days2 = 1;
    return '<LifecycleConfiguration ' +
        'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
        '<Rule>' +
        `<ID>${id1}</ID>` +
        `<Filter><Prefix>${prefix}</Prefix></Filter>` +
        '<Status>Enabled</Status>' +
        `<${action1}><Days>${days1}</Days></${action1}>` +
        '</Rule>' +
        '<Rule>' +
        `<ID>${id2}</ID>` +
        '<Filter><And>' +
        `<Prefix>${prefix}</Prefix>` +
        `<Tag><Key>${tags[0].key}</Key>` +
        `<Value>${tags[0].value}</Value></Tag>` +
        '</And></Filter>' +
        '<Status>Enabled</Status>' +
        `<${action2}><NoncurrentDays>${days2}</NoncurrentDays></${action2}>` +
        '</Rule>' +
        '</LifecycleConfiguration>';
}

const expectedLifecycleConfig = {
    rules: [
        {
            ruleID: 'test-id1',
            ruleStatus: 'Enabled',
            filter: {
                rulePrefix: 'test-prefix',
            },
            actions: [
                {
                    actionName: 'Expiration',
                    days: 365,
                },
            ],
        },
        {
            ruleID: 'test-id2',
            ruleStatus: 'Enabled',
            filter: {
                rulePrefix: 'test-prefix',
                tags: [
                    {
                        key: 'test-key1',
                        val: 'test-value1',
                    },
                ],
            },
            actions: [
                {
                    actionName: 'NoncurrentVersionExpiration',
                    days: 1,
                },
            ],
        },
    ],
};

describe('putBucketLifecycle API', () => {
    before(() => cleanup());
    beforeEach(done => bucketPut(authInfo, testBucketPutRequest, log, done));
    afterEach(() => cleanup());

    it('should update a bucket\'s metadata with lifecycle config obj', done => {
        const testPutLifecycleRequest = _getPutLifecycleRequest(
            _getTestLifecycleXml());
        bucketPutLifecycle(authInfo, testPutLifecycleRequest, log, err => {
            if (err) {
                process.stdout.write(`Err putting lifecycle config ${err}`);
                return done(err);
            }
            return metadata.getBucket(bucketName, log, (err, bucket) => {
                if (err) {
                    process.stdout.write(`Err retrieving bucket MD ${err}`);
                    return done(err);
                }
                const bucketLifecycleConfig =
                    bucket.getLifecycleConfiguration();
                assert.deepStrictEqual(
                    bucketLifecycleConfig, expectedLifecycleConfig);
                return done();
            });
        });
    });
});
