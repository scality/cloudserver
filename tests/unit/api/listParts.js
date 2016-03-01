import assert from 'assert';

import { parseString } from 'xml2js';

import Bucket from '../../../lib/metadata/in_memory/Bucket';
import constants from '../../../constants';
import { DummyRequestLogger, makeAuthInfo } from '../helpers';
import listParts from '../../../lib/api/listParts';
import metadata from '../metadataswitch';

const log = new DummyRequestLogger();

const splitter = constants.splitter;

const canonicalID = 'accessKey1';
const authInfo = makeAuthInfo(canonicalID);
const namespace = 'default';
const uploadId = '4db92ccc-d89d-49d3-9fa6-e9c2c1eb31b0';
const bucketName = 'freshestbucket';
const mpuBucket = `${constants.mpuBucketPrefix}${bucketName}`;
const uploadKey = '$makememulti';
const sixMBObjectETag = 'f3a9fb2071d3503b703938a74eb99846';
const lastPieceETag = '555e4cd2f9eff38109d7a3ab13995a32';
const overviewKey = `overview${splitter}$makememulti${splitter}4db92ccc-` +
    `d89d-49d3-9fa6-e9c2c1eb31b0`;
const partOneKey = `4db92ccc-d89d-49d3-9fa6-e9c2c1eb31b0${splitter}1`;
const partTwoKey = `4db92ccc-d89d-49d3-9fa6-e9c2c1eb31b0` +
    `${splitter}2`;
const partThreeKey = `4db92ccc-d89d-49d3-9fa6-e9c2c1eb31b0${splitter}3`;
const partFourKey = `4db92ccc-d89d-49d3-9fa6-e9c2c1eb31b0${splitter}4`;
const partFiveKey = `4db92ccc-d89d-49d3-9fa6-e9c2c1eb31b0${splitter}5`;

describe('List Parts API', () => {
    beforeEach(done => {
        const sampleNormalBucketInstance = new Bucket(bucketName,
            canonicalID, authInfo.getAccountDisplayName());
        const sampleMPUInstance = new Bucket(mpuBucket, 'admin', 'admin');
        sampleMPUInstance.keyMap[overviewKey] = {
            id: '4db92ccc-d89d-49d3-9fa6-e9c2c1eb31b0',
            'owner-display-name': authInfo.getAccountDisplayName(),
            'owner-id': canonicalID,
            initiator: {
                DisplayName: authInfo.getAccountDisplayName(),
                ID: canonicalID,
            },
            key: '$makememulti',
            initiated: '2015-11-30T22:40:07.858Z',
            uploadId: '4db92ccc-d89d-49d3-9fa6-e9c2c1eb31b0',
            acl: {
                Canned: 'private',
                FULL_CONTROL: [],
                WRITE_ACP: [],
                READ: [],
                READ_ACP: [],
            },
            eventualStorageBucket: 'freshestbucket',
        };

        sampleMPUInstance.keyMap[partOneKey] = {
            key: partOneKey,
            'last-modified': '2015-11-30T22:41:18.658Z',
            'content-md5': 'f3a9fb2071d3503b703938a74eb99846',
            'content-length': '6000000',
            partLocations: ['068db6a6745a79d54c1b29ff99f9f131'],
        };
        sampleMPUInstance.keyMap[partTwoKey] = {
            key: partTwoKey,
            'last-modified': '2015-11-30T22:41:40.207Z',
            'content-md5': 'f3a9fb2071d3503b703938a74eb99846',
            'content-length': '6000000',
            partLocations: ['ff22f316b16956ff5118c93abce7d62d'],
        };
        sampleMPUInstance.keyMap[partThreeKey] = {
            key: partThreeKey,
            'last-modified': '2015-11-30T22:41:52.102Z',
            'content-md5': 'f3a9fb2071d3503b703938a74eb99846',
            'content-length': '6000000',
            partLocations: ['dea282f70edb6fc5f9433cd6f525d4a6'],
        };
        sampleMPUInstance.keyMap[partFourKey] = {
            key: partFourKey,
            'last-modified': '2015-11-30T22:42:03.493Z',
            'content-md5': 'f3a9fb2071d3503b703938a74eb99846',
            'content-length': '6000000',
            partLocations: ['afe24bc40153982e1f7f28066f7af6a4'],
        };
        sampleMPUInstance.keyMap[partFiveKey] = {
            key: partFiveKey,
            'last-modified': '2015-11-30T22:42:22.876Z',
            'content-md5': '555e4cd2f9eff38109d7a3ab13995a32',
            'content-length': '18',
            partLocations: ['85bc16f5769687070fb13cfe66b5e41f'],
        };

        metadata.createBucket(bucketName, sampleNormalBucketInstance, log,
            () => {
                metadata.createBucket(mpuBucket, sampleMPUInstance, log, done);
            });
    });

    afterEach(done => {
        metadata.deleteBucket(bucketName, log, () => {
            metadata.deleteBucket(mpuBucket, log, done);
        });
    });

    it('should list all parts of a multipart upload', done => {
        const listRequest = {
            bucketName,
            namespace,
            objectKey: uploadKey,
            url: `/${uploadKey}?uploadId=${uploadId}`,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            query: { uploadId },
        };

        listParts(authInfo, listRequest, log, (err, xml) => {
            assert.strictEqual(err, null);
            parseString(xml, (err, json) => {
                assert.strictEqual(err, null);
                assert.strictEqual(json.ListPartResult.Bucket[0], bucketName);
                assert.strictEqual(json.ListPartResult.Key[0], uploadKey);
                assert.strictEqual(json.ListPartResult.UploadId[0], uploadId);
                assert.strictEqual(json.ListPartResult.MaxParts[0], '1000');
                assert.strictEqual(json.ListPartResult.Initiator[0].ID[0],
                                   authInfo.getCanonicalID());
                assert.strictEqual(json.ListPartResult.IsTruncated[0], 'false');
                assert.strictEqual(json.ListPartResult.PartNumberMarker,
                                   undefined);
                assert.strictEqual(json.ListPartResult.NextPartNumberMarker,
                                   undefined);
                assert.strictEqual(json.ListPartResult.Part[0].PartNumber[0],
                                   '1');
                assert.strictEqual(json.ListPartResult.Part[0].ETag[0],
                                   sixMBObjectETag);
                assert.strictEqual(json.ListPartResult.Part[0].Size[0],
                                   '6000000');
                assert.strictEqual(json.ListPartResult.Part[4].PartNumber[0],
                                   '5');
                assert.strictEqual(json.ListPartResult.Part[4].ETag[0],
                                   lastPieceETag);
                assert.strictEqual(json.ListPartResult.Part[4].Size[0], '18');
                assert.strictEqual(json.ListPartResult.Part.length, 5);
                done();
            });
        });
    });

    it('should return xml with objectKey url encoded if requested', done => {
        const listRequest = {
            bucketName,
            namespace,
            objectKey: uploadKey,
            url: `/${uploadKey}?uploadId=${uploadId}`,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            query: {
                uploadId,
                'encoding-type': 'url',
            },
        };
        const urlEncodedObjectKey = '%24makememulti';

        listParts(authInfo, listRequest, log, (err, xml) => {
            assert.strictEqual(err, null);
            parseString(xml, (err, json) => {
                assert.strictEqual(json.ListPartResult.Key[0],
                                   urlEncodedObjectKey);
                done();
            });
        });
    });

    it('should list only up to requested number ' +
    'of max parts of a multipart upload', done => {
        const listRequest = {
            bucketName,
            namespace,
            objectKey: uploadKey,
            url: `/${uploadKey}?uploadId=${uploadId}`,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            query: {
                uploadId,
                'max-parts': '4',
            },
        };

        listParts(authInfo, listRequest, log, (err, xml) => {
            assert.strictEqual(err, null);
            parseString(xml, (err, json) => {
                assert.strictEqual(err, null);
                assert.strictEqual(json.ListPartResult.Bucket[0], bucketName);
                assert.strictEqual(json.ListPartResult.Key[0], uploadKey);
                assert.strictEqual(json.ListPartResult.UploadId[0], uploadId);
                assert.strictEqual(json.ListPartResult.MaxParts[0], '4');
                assert.strictEqual(json.ListPartResult.Initiator[0].ID[0],
                                   authInfo.getCanonicalID());
                assert.strictEqual(json.ListPartResult.IsTruncated[0], 'true');
                assert.strictEqual(json.ListPartResult.PartNumberMarker,
                                   undefined);
                assert.strictEqual(json.ListPartResult.NextPartNumberMarker[0],
                                   '4');
                assert.strictEqual(json.ListPartResult.Part[2].PartNumber[0],
                                   '3');
                assert.strictEqual(json.ListPartResult.Part[2].ETag[0],
                                   sixMBObjectETag);
                assert.strictEqual(json.ListPartResult.Part[2].Size[0],
                                   '6000000');
                assert.strictEqual(json.ListPartResult.Part.length, 4);
                done();
            });
        });
    });

    it('should list all parts if requested max-parts ' +
    'is greater than total number of parts', done => {
        const listRequest = {
            bucketName,
            namespace,
            objectKey: uploadKey,
            url: `/${uploadKey}?uploadId=${uploadId}`,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            query: {
                uploadId,
                'max-parts': '6',
            },
        };

        listParts(authInfo, listRequest, log, (err, xml) => {
            assert.strictEqual(err, null);
            parseString(xml, (err, json) => {
                assert.strictEqual(err, null);
                assert.strictEqual(json.ListPartResult.Bucket[0], bucketName);
                assert.strictEqual(json.ListPartResult.Key[0], uploadKey);
                assert.strictEqual(json.ListPartResult.UploadId[0], uploadId);
                assert.strictEqual(json.ListPartResult.MaxParts[0], '6');
                assert.strictEqual(json.ListPartResult.Initiator[0].ID[0],
                                   authInfo.getCanonicalID());
                assert.strictEqual(json.ListPartResult.IsTruncated[0], 'false');
                assert.strictEqual(json.ListPartResult.PartNumberMarker,
                                   undefined);
                assert.strictEqual(json.ListPartResult.NextPartNumberMarker,
                                   undefined);
                assert.strictEqual(json.ListPartResult.Part[2].PartNumber[0],
                                   '3');
                assert.strictEqual(json.ListPartResult.Part[2].ETag[0],
                                   sixMBObjectETag);
                assert.strictEqual(json.ListPartResult.Part[2].Size[0],
                                   '6000000');
                assert.strictEqual(json.ListPartResult.Part.length, 5);
                done();
            });
        });
    });

    it('should only list parts after PartNumberMarker', done => {
        const listRequest = {
            bucketName,
            namespace,
            objectKey: uploadKey,
            url: `/${uploadKey}?uploadId=${uploadId}`,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            query: {
                uploadId,
                'part-number-marker': '2',
            },
        };

        listParts(authInfo, listRequest, log, (err, xml) => {
            assert.strictEqual(err, null);
            parseString(xml, (err, json) => {
                assert.strictEqual(err, null);
                assert.strictEqual(json.ListPartResult.Bucket[0], bucketName);
                assert.strictEqual(json.ListPartResult.Key[0], uploadKey);
                assert.strictEqual(json.ListPartResult.UploadId[0], uploadId);
                assert.strictEqual(json.ListPartResult.MaxParts[0], '1000');
                assert.strictEqual(json.ListPartResult.Initiator[0].ID[0],
                                   authInfo.getCanonicalID());
                assert.strictEqual(json.ListPartResult.IsTruncated[0], 'false');
                assert.strictEqual(json.ListPartResult.PartNumberMarker[0],
                                   '2');
                assert.strictEqual(json.ListPartResult.NextPartNumberMarker,
                                   undefined);
                assert.strictEqual(json.ListPartResult.Part[0].PartNumber[0],
                                   '3');
                assert.strictEqual(json.ListPartResult.Part[0].ETag[0],
                                   sixMBObjectETag);
                assert.strictEqual(json.ListPartResult.Part[0].Size[0],
                                   '6000000');
                assert.strictEqual(json.ListPartResult.Part[2].PartNumber[0],
                                   '5');
                assert.strictEqual(json.ListPartResult.Part.length, 3);
                done();
            });
        });
    });

    it('should handle a part-number-marker specified ' +
    'and a max-parts specified', done => {
        const listRequest = {
            bucketName,
            namespace,
            objectKey: uploadKey,
            url: `/${uploadKey}?uploadId=${uploadId}`,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            query: {
                uploadId,
                'part-number-marker': '2',
                'max-parts': '2',
            }
        };

        listParts(authInfo, listRequest, log, (err, xml) => {
            assert.strictEqual(err, null);
            parseString(xml, (err, json) => {
                assert.strictEqual(err, null);
                assert.strictEqual(json.ListPartResult.Bucket[0], bucketName);
                assert.strictEqual(json.ListPartResult.Key[0], uploadKey);
                assert.strictEqual(json.ListPartResult.UploadId[0], uploadId);
                assert.strictEqual(json.ListPartResult.MaxParts[0], '2');
                assert.strictEqual(json.ListPartResult.Initiator[0].ID[0],
                                   authInfo.getCanonicalID());
                assert.strictEqual(json.ListPartResult.IsTruncated[0], 'true');
                assert.strictEqual(json.ListPartResult.PartNumberMarker[0],
                                   '2');
                assert.strictEqual(json.ListPartResult.NextPartNumberMarker[0],
                                   '4');
                assert.strictEqual(json.ListPartResult.Part[0].PartNumber[0],
                                   '3');
                assert.strictEqual(json.ListPartResult.Part[0].ETag[0],
                                   sixMBObjectETag);
                assert.strictEqual(json.ListPartResult.Part[0].Size[0],
                                   '6000000');
                assert.strictEqual(json.ListPartResult.Part[1].PartNumber[0],
                                   '4');
                assert.strictEqual(json.ListPartResult.Part.length, 2);
                done();
            });
        });
    });
});
