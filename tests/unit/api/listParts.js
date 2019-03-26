const assert = require('assert');

const { parseString } = require('xml2js');

const BucketInfo = require('arsenal').models.BucketInfo;
const constants = require('../../../constants');
const { cleanup, DummyRequestLogger, makeAuthInfo } = require('../helpers');
const inMemMetadata
    = require('arsenal').storage.metadata.inMemory.metadata.metadata;
const listParts = require('../../../lib/api/listParts');
const metadata = require('../metadataswitch');

const log = new DummyRequestLogger();

const splitter = constants.splitter;

const accessKey = 'accessKey1';
const authInfo = makeAuthInfo(accessKey);
const canonicalID = authInfo.getCanonicalID();
const namespace = 'default';
const uploadId = '4db92ccc-d89d-49d3-9fa6-e9c2c1eb31b0';
const bucketName = 'freshestbucket';
const mpuBucket = `${constants.mpuBucketPrefix}${bucketName}`;
const uploadKey = '$makememulti';
const sixMBObjectETag = '"f3a9fb2071d3503b703938a74eb99846"';
const lastPieceETag = '"555e4cd2f9eff38109d7a3ab13995a32"';
const overviewKey = `overview${splitter}$makememulti${splitter}4db92ccc-` +
    'd89d-49d3-9fa6-e9c2c1eb31b0';
const partOneKey = `4db92ccc-d89d-49d3-9fa6-e9c2c1eb31b0${splitter}00001`;
const partTwoKey = '4db92ccc-d89d-49d3-9fa6-e9c2c1eb31b0' +
    `${splitter}00002`;
const partThreeKey = `4db92ccc-d89d-49d3-9fa6-e9c2c1eb31b0${splitter}00003`;
const partFourKey = `4db92ccc-d89d-49d3-9fa6-e9c2c1eb31b0${splitter}00004`;
const partFiveKey = `4db92ccc-d89d-49d3-9fa6-e9c2c1eb31b0${splitter}00005`;

describe('List Parts API', () => {
    beforeEach(done => {
        cleanup();
        const creationDate = new Date().toJSON();
        const sampleNormalBucketInstance = new BucketInfo(bucketName,
            canonicalID, authInfo.getAccountDisplayName(), creationDate,
            BucketInfo.currentModelVersion());
        const sampleMPUInstance = new BucketInfo(mpuBucket,
            'admin', 'admin', creationDate, BucketInfo.currentModelVersion());
        metadata.createBucket(bucketName, sampleNormalBucketInstance, log,
            () => {
                metadata.createBucket(mpuBucket, sampleMPUInstance, log, () => {
                    inMemMetadata.keyMaps.get(mpuBucket).set(overviewKey, {
                        'id': '4db92ccc-d89d-49d3-9fa6-e9c2c1eb31b0',
                        'owner-display-name': authInfo.getAccountDisplayName(),
                        'owner-id': canonicalID,
                        'initiator': {
                            DisplayName: authInfo.getAccountDisplayName(),
                            ID: canonicalID,
                        },
                        'key': '$makememulti',
                        'initiated': '2015-11-30T22:40:07.858Z',
                        'uploadId': '4db92ccc-d89d-49d3-9fa6-e9c2c1eb31b0',
                        'acl': {
                            Canned: 'private',
                            FULL_CONTROL: [],
                            WRITE_ACP: [],
                            READ: [],
                            READ_ACP: [],
                        },
                        'eventualStorageBucket': 'freshestbucket',
                        'mdBucketModelVersion': 2,
                    });

                    inMemMetadata.keyMaps.get(mpuBucket).set(partOneKey, {
                        'key': partOneKey,
                        'last-modified': '2015-11-30T22:41:18.658Z',
                        'content-md5': 'f3a9fb2071d3503b703938a74eb99846',
                        'content-length': '6000000',
                        'partLocations': ['068db6a6745a79d54c1b29ff99f9f131'],
                    });
                    inMemMetadata.keyMaps.get(mpuBucket).set(partTwoKey, {
                        'key': partTwoKey,
                        'last-modified': '2015-11-30T22:41:40.207Z',
                        'content-md5': 'f3a9fb2071d3503b703938a74eb99846',
                        'content-length': '6000000',
                        'partLocations': ['ff22f316b16956ff5118c93abce7d62d'],
                    });
                    inMemMetadata.keyMaps.get(mpuBucket).set(partThreeKey, {
                        'key': partThreeKey,
                        'last-modified': '2015-11-30T22:41:52.102Z',
                        'content-md5': 'f3a9fb2071d3503b703938a74eb99846',
                        'content-length': '6000000',
                        'partLocations': ['dea282f70edb6fc5f9433cd6f525d4a6'],
                    });
                    inMemMetadata.keyMaps.get(mpuBucket).set(partFourKey, {
                        'key': partFourKey,
                        'last-modified': '2015-11-30T22:42:03.493Z',
                        'content-md5': 'f3a9fb2071d3503b703938a74eb99846',
                        'content-length': '6000000',
                        'partLocations': ['afe24bc40153982e1f7f28066f7af6a4'],
                    });
                    inMemMetadata.keyMaps.get(mpuBucket).set(partFiveKey, {
                        'key': partFiveKey,
                        'last-modified': '2015-11-30T22:42:22.876Z',
                        'content-md5': '555e4cd2f9eff38109d7a3ab13995a32',
                        'content-length': '18',
                        'partLocations': ['85bc16f5769687070fb13cfe66b5e41f'],
                    });
                    done();
                });
            });
    });

    test('should list all parts of a multipart upload', done => {
        const listRequest = {
            bucketName,
            namespace,
            objectKey: uploadKey,
            url: `/${uploadKey}?uploadId=${uploadId}`,
            headers: { host: `${bucketName}.s3.amazonaws.com` },
            query: { uploadId },
        };

        listParts(authInfo, listRequest, log, (err, xml) => {
            expect(err).toBe(null);
            parseString(xml, (err, json) => {
                expect(err).toBe(null);
                expect(json.ListPartsResult.Bucket[0]).toBe(bucketName);
                expect(json.ListPartsResult.Key[0]).toBe(uploadKey);
                expect(json.ListPartsResult.UploadId[0]).toBe(uploadId);
                expect(json.ListPartsResult.MaxParts[0]).toBe('1000');
                expect(json.ListPartsResult.Initiator[0].ID[0]).toBe(authInfo.getCanonicalID());
                expect(json.ListPartsResult.IsTruncated[0]).toBe('false');
                expect(json.ListPartsResult.PartNumberMarker).toBe(undefined);
                expect(json.ListPartsResult.NextPartNumberMarker).toBe(undefined);
                expect(json.ListPartsResult.Part[0].PartNumber[0]).toBe('1');
                expect(json.ListPartsResult.Part[0].ETag[0]).toBe(sixMBObjectETag);
                expect(json.ListPartsResult.Part[0].Size[0]).toBe('6000000');
                expect(json.ListPartsResult.Part[4].PartNumber[0]).toBe('5');
                expect(json.ListPartsResult.Part[4].ETag[0]).toBe(lastPieceETag);
                expect(json.ListPartsResult.Part[4].Size[0]).toBe('18');
                expect(json.ListPartsResult.Part.length).toBe(5);
                done();
            });
        });
    });

    test('should return xml with objectKey url encoded if requested', done => {
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
            expect(err).toBe(null);
            parseString(xml, (err, json) => {
                expect(json.ListPartsResult.Key[0]).toBe(urlEncodedObjectKey);
                done();
            });
        });
    });

    test('should list only up to requested number ' +
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
            expect(err).toBe(null);
            parseString(xml, (err, json) => {
                expect(err).toBe(null);
                expect(json.ListPartsResult.Bucket[0]).toBe(bucketName);
                expect(json.ListPartsResult.Key[0]).toBe(uploadKey);
                expect(json.ListPartsResult.UploadId[0]).toBe(uploadId);
                expect(json.ListPartsResult.MaxParts[0]).toBe('4');
                expect(json.ListPartsResult.Initiator[0].ID[0]).toBe(authInfo.getCanonicalID());
                expect(json.ListPartsResult.IsTruncated[0]).toBe('true');
                expect(json.ListPartsResult.PartNumberMarker).toBe(undefined);
                expect(json.ListPartsResult.NextPartNumberMarker[0]).toBe('4');
                expect(json.ListPartsResult.Part[2].PartNumber[0]).toBe('3');
                expect(json.ListPartsResult.Part[2].ETag[0]).toBe(sixMBObjectETag);
                expect(json.ListPartsResult.Part[2].Size[0]).toBe('6000000');
                expect(json.ListPartsResult.Part.length).toBe(4);
                done();
            });
        });
    });

    test('should list all parts if requested max-parts ' +
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
            expect(err).toBe(null);
            parseString(xml, (err, json) => {
                expect(err).toBe(null);
                expect(json.ListPartsResult.Bucket[0]).toBe(bucketName);
                expect(json.ListPartsResult.Key[0]).toBe(uploadKey);
                expect(json.ListPartsResult.UploadId[0]).toBe(uploadId);
                expect(json.ListPartsResult.MaxParts[0]).toBe('6');
                expect(json.ListPartsResult.Initiator[0].ID[0]).toBe(authInfo.getCanonicalID());
                expect(json.ListPartsResult.IsTruncated[0]).toBe('false');
                expect(json.ListPartsResult.PartNumberMarker).toBe(undefined);
                expect(json.ListPartsResult.NextPartNumberMarker).toBe(undefined);
                expect(json.ListPartsResult.Part[2].PartNumber[0]).toBe('3');
                expect(json.ListPartsResult.Part[2].ETag[0]).toBe(sixMBObjectETag);
                expect(json.ListPartsResult.Part[2].Size[0]).toBe('6000000');
                expect(json.ListPartsResult.Part.length).toBe(5);
                done();
            });
        });
    });

    test('should only list parts after PartNumberMarker', done => {
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
            expect(err).toBe(null);
            parseString(xml, (err, json) => {
                expect(err).toBe(null);
                expect(json.ListPartsResult.Bucket[0]).toBe(bucketName);
                expect(json.ListPartsResult.Key[0]).toBe(uploadKey);
                expect(json.ListPartsResult.UploadId[0]).toBe(uploadId);
                expect(json.ListPartsResult.MaxParts[0]).toBe('1000');
                expect(json.ListPartsResult.Initiator[0].ID[0]).toBe(authInfo.getCanonicalID());
                expect(json.ListPartsResult.IsTruncated[0]).toBe('false');
                expect(json.ListPartsResult.PartNumberMarker[0]).toBe('2');
                expect(json.ListPartsResult.NextPartNumberMarker).toBe(undefined);
                expect(json.ListPartsResult.Part[0].PartNumber[0]).toBe('3');
                expect(json.ListPartsResult.Part[0].ETag[0]).toBe(sixMBObjectETag);
                expect(json.ListPartsResult.Part[0].Size[0]).toBe('6000000');
                expect(json.ListPartsResult.Part[2].PartNumber[0]).toBe('5');
                expect(json.ListPartsResult.Part.length).toBe(3);
                done();
            });
        });
    });

    test('should handle a part-number-marker specified ' +
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
            },
        };

        listParts(authInfo, listRequest, log, (err, xml) => {
            expect(err).toBe(null);
            parseString(xml, (err, json) => {
                expect(err).toBe(null);
                expect(json.ListPartsResult.Bucket[0]).toBe(bucketName);
                expect(json.ListPartsResult.Key[0]).toBe(uploadKey);
                expect(json.ListPartsResult.UploadId[0]).toBe(uploadId);
                expect(json.ListPartsResult.MaxParts[0]).toBe('2');
                expect(json.ListPartsResult.Initiator[0].ID[0]).toBe(authInfo.getCanonicalID());
                expect(json.ListPartsResult.IsTruncated[0]).toBe('true');
                expect(json.ListPartsResult.PartNumberMarker[0]).toBe('2');
                expect(json.ListPartsResult.NextPartNumberMarker[0]).toBe('4');
                expect(json.ListPartsResult.Part[0].PartNumber[0]).toBe('3');
                expect(json.ListPartsResult.Part[0].ETag[0]).toBe(sixMBObjectETag);
                expect(json.ListPartsResult.Part[0].Size[0]).toBe('6000000');
                expect(json.ListPartsResult.Part[1].PartNumber[0]).toBe('4');
                expect(json.ListPartsResult.Part.length).toBe(2);
                done();
            });
        });
    });
});
