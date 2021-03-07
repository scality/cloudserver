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
}, {
    action: 'listBucket',
    metrics: {
        bucket: 'bucket1',
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
}, {
    action: 'listBucket',
    metrics: {
        bucket: 'bucket1',
    },
}, {
    action: 'headObject',
    metrics: {
        bucket: 'bucket1',
        keys: ['1.txt'],
        versionId: undefined,
        location: 'us-west-1',
    }
}, {
    action: 'abortMultipartUpload',
    metrics: {
        bucket: 'destinationbucket815502017',
        keys: ['copycatobject'],
        byteLength: 26,
        location: 'us-east-1',
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
}, {
    action: 'createBucket',
    metrics: {
        bucket: 'deletebucketpolicy-test-bucket',
    },
}, {
    action: 'deleteBucket',
    metrics: {
        bucket: 'deletebucketpolicy-test-bucket',
    },
}, {
    action: 'deleteBucketCors',
    metrics: {
        bucket: 'testdeletecorsbucket',
    },
}, {
    action: 'deleteBucketReplication',
    metrics: {
        bucket: 'source-bucket',
    },
}, {
    action: 'deleteBucketWebsite',
    metrics: {
        bucket: 'testdeletewebsitebucket',
    },
}, {
    action: 'getBucketAcl',
    metrics: {
        bucket: 'putbucketaclfttest',
    },
}, {
    action: 'getBucketCors',
    metrics: {
        bucket: 'testgetcorsbucket',
    },
}, {
    action: 'getBucketLocation',
    metrics: {
        bucket: 'testgetlocationbucket',
    },
}, {
    action: 'getBucketNotification',
    metrics: {
        bucket: 'notificationtestbucket',
    },
}, {
    action: 'getBucketObjectLock',
    metrics: {
        bucket: 'mock-bucket',
    },
}, {
    action: 'getBucketReplication',
    metrics: {
        bucket: 'source-bucket',
    },
}, {
    action: 'getBucketVersioning',
    metrics: {
        bucket: 'bucket-with-object-lock',
    },
}, {
    action: 'getBucketWebsite',
    metrics: {
        bucket: 'testgetwebsitetestbucket',
    },
}, {
    action: 'getObjectTagging',
    metrics: {
        bucket: 'completempu1615102906771',
        keys: ['keywithtags'],
        versionId: undefined,
        location: 'us-east-1',
    },
}, {
    action: 'headObject',
    metrics: {
        bucket: 'supersourcebucket81033016532',
        keys: ['supersourceobject'],
        versionId: undefined,
        location: 'us-east-1',
    },
}, {
    action: 'initiateMultipartUpload',
    metrics: {
        bucket: 'destinationbucket815502017',
        keys: [ 'copycatobject' ],
        location: 'us-east-1',
    },
}, {
    action: 'listMultipartUploadParts',
    metrics: {
        bucket: 'ftest-mybucket-74',
    }
}, {
    action: 'multiObjectDelete',
    metrics: {
        bucket: 'completempu1615102906771',
        keys: [ undefined ],
        byteLength: 3,
        numberOfObjects: 1,
        isDelete: true,
    },
}, {
    action: 'putBucketAcl',
    metrics: {
        bucket: 'putbucketaclfttest',
    },
}, {
    action: 'putBucketCors',
    metrics: {
        bucket: 'testcorsbucket',
    },
}, {
    action: 'putBucketNotification',
    metrics: {
        bucket: 'notificationtestbucket',
    },
}, {
    action: 'putBucketObjectLock',
    metrics: {
        bucket: 'mock-bucket',
    },
}, {
    action: 'putBucketReplication',
    metrics: {
        bucket: 'source-bucket',
    },
}, {
    action: 'putBucketVersioning',
    metrics: {
        bucket: 'source-bucket',
    },
}, {
    action: 'putBucketWebsite',
    metrics: {
        bucket: 'testgetwebsitetestbucket',
    },
}, {
    action: 'uploadPart',
    metrics: {
        bucket: 'ftest-mybucket-74',
        keys: [ 'toAbort&<>"\'' ],
        newByteLength: 5242880,
        oldByteLength: null,
        location: 'us-east-1',
    },
}, {
    action: 'uploadPartCopy',
    metrics: {
        bucket: 'destinationbucket815502017',
        keys: [ 'copycatobject' ],
        newByteLength: 26,
        oldByteLength: null,
        location: 'us-east-1',
    },
}];

function computeMetrics(action, event) {
    const {
        numberOfObjects,
        oldByteLength,
        newByteLength,
        byteLength,
        isDelete,
    } = event;
    const sizeDelta = oldByteLength ? newByteLength - oldByteLength :
        (action === 'getObject' ? 0 : newByteLength);
    return {
        objectDelta: isDelete ? -numberOfObjects : numberOfObjects,
        sizeDelta: isDelete ? -byteLength : sizeDelta,
        incomingBytes: action === 'getObject' ? 0 : newByteLength,
        outgoingBytes: action === 'getObject' ? newByteLength : 0,
    };
}

describe('utapi v2 pushmetrics utility', function healthCheck() {
    const log = new werelogs.Logger('utapi-utility');
    const { UtapiClient, utapiVersion } = require('utapi');
    let pushMetric;

    function pushMetricStub(event) {
        return event;
    }

    before(() => {
        assert.strictEqual(utapiVersion, 2);
        sinon.stub(UtapiClient.prototype, "pushMetric")
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
            const compareObj = computeMetrics(event.action, event.metrics);
            assert(compareObj);
            Object.keys(compareObj).forEach(key => {
                assert.strictEqual(eventPushed[key], compareObj[key]);
            });
        });
    });
});
