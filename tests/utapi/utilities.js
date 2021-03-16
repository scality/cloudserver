const assert = require('assert');
const sinon = require('sinon');
const werelogs = require('werelogs');

const testEvents = [{
    action: 'getObject',
    metrics: {
        bucket: 'bucket1',
        keys: ['1.txt'],
        newByteLength: 2,
        oldByteLength: null,
        versionId: undefined,
        location: 'us-west-1',
        numberOfObjects: 1,
        byteLength: null,
        isDelete: false,
    },
    expected: {
        objectDelta: 1,
        sizeDelta: 0,
        incomingBytes: 0,
        outgoingBytes: 2,
    },
}, {
    action: 'deleteObject',
    metrics: {
        bucket: 'bucket1',
        keys: ['1.txt'],
        byteLength: 2,
        numberOfObjects: 1,
        location: 'us-west-1',
        isDelete: true,
    },
    expected: {
        objectDelta: -1,
        sizeDelta: -2,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'listBucket',
    metrics: {
        bucket: 'bucket1',
    },
    expected: {
        objectDelta: undefined,
        sizeDelta: undefined,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'putObject',
    metrics: {
        bucket: 'bucket1',
        keys: ['2.txt'],
        newByteLength: 2,
        oldByteLength: null,
        versionId: undefined,
        location: 'us-west-1',
        numberOfObjects: 1,
    },
    expected: {
        objectDelta: 1,
        sizeDelta: 2,
        incomingBytes: 2,
        outgoingBytes: 0,
    },
}, {
    action: 'listBucket',
    metrics: {
        bucket: 'bucket1',
    },
    expected: {
        objectDelta: undefined,
        sizeDelta: undefined,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'headObject',
    metrics: {
        bucket: 'bucket1',
        keys: ['1.txt'],
        versionId: undefined,
        location: 'us-west-1',
    },
    expected: {
        objectDelta: undefined,
        sizeDelta: undefined,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'abortMultipartUpload',
    metrics: {
        bucket: 'destinationbucket815502017',
        keys: ['copycatobject'],
        byteLength: 26,
        location: 'us-east-1',
    },
    expected: {
        objectDelta: undefined,
        sizeDelta: -26,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'completeMultipartUpload',
    metrics: {
        oldByteLength: null,
        bucket: 'destinationbucket815502017',
        keys: ['copycatobject'],
        versionId: undefined,
        numberOfObjects: 1,
        location: 'us-east-1',
    },
    expected: {
        objectDelta: 1,
        sizeDelta: undefined,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'createBucket',
    metrics: {
        bucket: 'deletebucketpolicy-test-bucket',
    },
    expected: {
        objectDelta: undefined,
        sizeDelta: undefined,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'deleteBucket',
    metrics: {
        bucket: 'deletebucketpolicy-test-bucket',
    },
    expected: {
        objectDelta: undefined,
        sizeDelta: undefined,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'deleteBucketCors',
    metrics: {
        bucket: 'testdeletecorsbucket',
    },
    expected: {
        objectDelta: undefined,
        sizeDelta: undefined,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'deleteBucketReplication',
    metrics: {
        bucket: 'source-bucket',
    },
    expected: {
        objectDelta: undefined,
        sizeDelta: undefined,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'deleteBucketWebsite',
    metrics: {
        bucket: 'testdeletewebsitebucket',
    },
    expected: {
        objectDelta: undefined,
        sizeDelta: undefined,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'getBucketAcl',
    metrics: {
        bucket: 'putbucketaclfttest',
    },
    expected: {
        objectDelta: undefined,
        sizeDelta: undefined,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'getBucketCors',
    metrics: {
        bucket: 'testgetcorsbucket',
    },
    expected: {
        objectDelta: undefined,
        sizeDelta: undefined,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'getBucketLocation',
    metrics: {
        bucket: 'testgetlocationbucket',
    },
    expected: {
        objectDelta: undefined,
        sizeDelta: undefined,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'getBucketNotification',
    metrics: {
        bucket: 'notificationtestbucket',
    },
    expected: {
        objectDelta: undefined,
        sizeDelta: undefined,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'getBucketObjectLock',
    metrics: {
        bucket: 'mock-bucket',
    },
    expected: {
        objectDelta: undefined,
        sizeDelta: undefined,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'getBucketReplication',
    metrics: {
        bucket: 'source-bucket',
    },
    expected: {
        objectDelta: undefined,
        sizeDelta: undefined,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'getBucketVersioning',
    metrics: {
        bucket: 'bucket-with-object-lock',
    },
    expected: {
        objectDelta: undefined,
        sizeDelta: undefined,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'getBucketWebsite',
    metrics: {
        bucket: 'testgetwebsitetestbucket',
    },
    expected: {
        objectDelta: undefined,
        sizeDelta: undefined,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'getObjectTagging',
    metrics: {
        bucket: 'completempu1615102906771',
        keys: ['keywithtags'],
        versionId: undefined,
        location: 'us-east-1',
    },
    expected: {
        objectDelta: undefined,
        sizeDelta: undefined,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'headObject',
    metrics: {
        bucket: 'supersourcebucket81033016532',
        keys: ['supersourceobject'],
        versionId: undefined,
        location: 'us-east-1',
    },
    expected: {
        objectDelta: undefined,
        sizeDelta: undefined,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'initiateMultipartUpload',
    metrics: {
        bucket: 'destinationbucket815502017',
        keys: ['copycatobject'],
        location: 'us-east-1',
    },
    expected: {
        objectDelta: undefined,
        sizeDelta: undefined,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'listMultipartUploadParts',
    metrics: {
        bucket: 'ftest-mybucket-74',
    },
    expected: {
        objectDelta: undefined,
        sizeDelta: undefined,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'multiObjectDelete',
    metrics: {
        bucket: 'completempu1615102906771',
        keys: [undefined],
        byteLength: 3,
        numberOfObjects: 1,
        isDelete: true,
    },
    expected: {
        objectDelta: -1,
        sizeDelta: -3,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'putBucketAcl',
    metrics: {
        bucket: 'putbucketaclfttest',
    },
    expected: {
        objectDelta: undefined,
        sizeDelta: undefined,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'putBucketCors',
    metrics: {
        bucket: 'testcorsbucket',
    },
    expected: {
        objectDelta: undefined,
        sizeDelta: undefined,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'putBucketNotification',
    metrics: {
        bucket: 'notificationtestbucket',
    },
    expected: {
        objectDelta: undefined,
        sizeDelta: undefined,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'putBucketObjectLock',
    metrics: {
        bucket: 'mock-bucket',
    },
    expected: {
        objectDelta: undefined,
        sizeDelta: undefined,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'putBucketReplication',
    metrics: {
        bucket: 'source-bucket',
    },
    expected: {
        objectDelta: undefined,
        sizeDelta: undefined,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'putBucketVersioning',
    metrics: {
        bucket: 'source-bucket',
    },
    expected: {
        objectDelta: undefined,
        sizeDelta: undefined,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'putBucketWebsite',
    metrics: {
        bucket: 'testgetwebsitetestbucket',
    },
    expected: {
        objectDelta: undefined,
        sizeDelta: undefined,
        incomingBytes: undefined,
        outgoingBytes: 0,
    },
}, {
    action: 'uploadPart',
    metrics: {
        bucket: 'ftest-mybucket-74',
        keys: ['toAbort&<>"\''],
        newByteLength: 5242880,
        oldByteLength: null,
        location: 'us-east-1',
    },
    expected: {
        objectDelta: undefined,
        sizeDelta: 5242880,
        incomingBytes: 5242880,
        outgoingBytes: 0,
    },
}, {
    action: 'uploadPartCopy',
    metrics: {
        bucket: 'destinationbucket815502017',
        keys: ['copycatobject'],
        newByteLength: 26,
        oldByteLength: null,
        location: 'us-east-1',
    },
    expected: {
        objectDelta: undefined,
        sizeDelta: 26,
        incomingBytes: 26,
        outgoingBytes: 0,
    },
}];

describe('utapi v2 pushmetrics utility', () => {
    const log = new werelogs.Logger('utapi-utility');
    const { UtapiClient, utapiVersion } = require('utapi');
    let pushMetric;

    function pushMetricStub(event) {
        return event;
    }

    before(() => {
        assert.strictEqual(utapiVersion, 2);
        sinon.stub(UtapiClient.prototype, 'pushMetric')
            .callsFake(pushMetricStub);
        pushMetric = require('../../lib/utapi/utilities').pushMetric;
    });

    after(() => {
        sinon.restore();
    });

    testEvents.forEach(event => {
        it(`should compute and push metrics for ${event.action}`, () => {
            const eventPushed = pushMetric(event.action, log, event.metrics);
            assert(eventPushed);
            Object.keys(event.expected).forEach(key => {
                assert.strictEqual(eventPushed[key], event.expected[key]);
            });
        });
    });
});
