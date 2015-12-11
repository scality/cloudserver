import { expect } from 'chai';
import { parseString } from 'xml2js';

import Bucket from '../../../lib/metadata/in_memory/Bucket';
import listParts from '../../../lib/api/listParts';
import metastore from '../../../lib/metadata/in_memory/metadata';
import config from '../../../config';
const splitter = config.splitter;

const accessKey = 'accessKey1';
const namespace = 'default';
const uploadId = '4db92ccc-d89d-49d3-9fa6-e9c2c1eb31b0';
const bucketName = 'freshestbucket';
const bucketUID = '0969df071dc0de6603230850ac138a30';
const mpuBucket = `mpu...${bucketUID}`;
const uploadKey = '$makememulti';
const sixMBObjectEtag = 'f3a9fb2071d3503b703938a74eb99846';
const lastPieceEtag = '555e4cd2f9eff38109d7a3ab13995a32';
const overviewKey = `overview${splitter}$makememulti${splitter}4db92ccc-` +
    `d89d-49d3-9fa6-e9c2c1eb31b0${splitter}freshestbucket` +
    `${splitter}accessKey1${splitter}placeholder display name for ` +
    `now${splitter}accessKey1${splitter}placeholder display name ` +
    `for now${splitter}undefined${splitter}2015-11-30T22:40:07.858Z`;
const partOneKey = `4db92ccc-d89d-49d3-9fa6-e9c2c1eb31b0${splitter}1` +
    `${splitter}2015-11-30T22:41:18.658Z${splitter}` +
    `f3a9fb2071d3503b703938a74eb99846` +
    `${splitter}6000000${splitter}068db6a6745a79d54c1b29ff99f9f131`;
const partTwoKey = `4db92ccc-d89d-49d3-9fa6-e9c2c1eb31b0` +
    `${splitter}2${splitter}2015-11-30T22:41:40.207Z${splitter}f3a9fb2071d35` +
    `03b703938a74eb99846${splitter}6000000${splitter}` +
    `ff22f316b16956ff5118c93abce7d62d`;
const partThreeKey = `4db92ccc-d89d-49d3-9fa6-e9c2c1eb31b0...` +
    `!*!3${splitter}2015-11-30T22:41:52.102Z` +
    `${splitter}f3a9fb2071d3503b703938a` +
    `74eb99846${splitter}6000000${splitter}dea282f70edb6fc5f9433cd6f525d4a6`;
const partFourKey = `4db92ccc-d89d-49d3-9fa6-e9c2c1eb31b0${splitter}4...` +
    `!*!2015-11-30T22:42:03.493Z${splitter}f3a9fb2071d3503b703938a74eb99846` +
    `${splitter}6000000${splitter}afe24bc40153982e1f7f28066f7af6a4`;
const partFiveKey = `4db92ccc-d89d-49d3-9fa6-e9c2c1eb31b0${splitter}5...` +
    `!*!2015-11-30T22:42:22.876Z${splitter}555e4cd2f9eff38109d7a3ab13995a32` +
    `${splitter}18${splitter}85bc16f5769687070fb13cfe66b5e41f`;

describe('List Parts API', () => {
    beforeEach(() => {
        const sampleNormalBucketInstance = new Bucket();
        sampleNormalBucketInstance.owner = accessKey;
        sampleNormalBucketInstance.name = bucketName;
        const sampleMPUInstance = new Bucket();
        sampleMPUInstance.owner = accessKey;
        sampleMPUInstance.name = mpuBucket;
        sampleMPUInstance.keyMap[overviewKey] = {
            "id": "4db92ccc-d89d-49d3-9fa6-e9c2c1eb31b0",
            "owner": {
                "displayName": "placeholder " +
                    "display name for now",
                "id": "accessKey1"
            },
            "initiator": {
                "displayName": "placeholder display " +
                    "name for now",
                "id": "accessKey1"
            },
            "key": "$makememulti",
            "initiated": "2015-11-30T22:40:07.858Z",
            "uploadId": "4db92ccc-d89d-49d3-9fa6-e9c2c1eb31b0",
            "acl": {
                "Canned": "private",
                "FULL_CONTROL": [],
                "WRITE_ACP": [],
                "READ": [],
                "READ_ACP": []
            }
        };
        sampleMPUInstance.keyMap[partOneKey] = '';
        sampleMPUInstance.keyMap[partTwoKey] = '';
        sampleMPUInstance.keyMap[partThreeKey] = '';
        sampleMPUInstance.keyMap[partFourKey] = '';
        sampleMPUInstance.keyMap[partFiveKey] = '';

        metastore.buckets[bucketUID] = sampleNormalBucketInstance;
        metastore.buckets[mpuBucket] = sampleMPUInstance;
    });

    it('should list all parts of a multipart upload', (done) => {
        const listRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: `/${uploadKey}?uploadId=${uploadId}`,
            namespace: namespace,
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            query: {
                uploadId: uploadId,
            }
        };

        listParts(accessKey, metastore, listRequest, (err, xml) => {
            expect(err).to.be.null;
            parseString(xml, (err, json) => {
                expect(err).to.be.null;
                expect(json.ListPartResult.Bucket[0]).to.equal(bucketName);
                expect(json.ListPartResult.Key[0]).to.equal(uploadKey);
                expect(json.ListPartResult.UploadId[0]).to.equal(uploadId);
                expect(json.ListPartResult.MaxParts[0]).to.equal('1000');
                expect(json.ListPartResult.Initiator[0]
                    .ID[0]).to.equal(accessKey);
                expect(json.ListPartResult.IsTruncated[0]).to.equal('false');
                expect(json.ListPartResult.PartNumberMarker).to.be.undefined;
                expect(json.ListPartResult
                    .NextPartNumberMarker).to.be.undefined;
                expect(json.ListPartResult.Part[0].PartNumber[0]).to.equal('1');
                expect(json.ListPartResult.Part[0].ETag[0])
                    .to.equal(sixMBObjectEtag);
                expect(json.ListPartResult.Part[0].Size[0])
                    .to.equal('6000000');
                expect(json.ListPartResult.Part[4].PartNumber[0]).to.equal('5');
                expect(json.ListPartResult.Part[4].ETag[0])
                    .to.equal(lastPieceEtag);
                expect(json.ListPartResult.Part[4].Size[0])
                    .to.equal('18');
                expect(json.ListPartResult.Part).to.have.length.of(5);
                done();
            });
        });
    });

    it('should return xml with objectKey url encoded if requested', (done) => {
        const listRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
            },
            url: `/${uploadKey}?uploadId=${uploadId}`,
            namespace: namespace,
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            query: {
                uploadId: uploadId,
                'encoding-type': 'url',
            }
        };
        const urlEncodedObjectKey = '%24makememulti';

        listParts(accessKey, metastore, listRequest, (err, xml) => {
            expect(err).to.be.null;
            parseString(xml, (err, json) => {
                expect(json.ListPartResult.Key[0])
                    .to.equal(urlEncodedObjectKey);
                done();
            });
        });
    });

    it('should list only up to requested number ' +
    'of max parts of a multipart upload', (done) => {
        const listRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
            },
            url: `/${uploadKey}?uploadId=${uploadId}`,
            namespace: namespace,
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            query: {
                uploadId: uploadId,
                'max-parts': '4',
            }
        };

        listParts(accessKey, metastore, listRequest, (err, xml) => {
            expect(err).to.be.null;
            parseString(xml, (err, json) => {
                expect(err).to.be.null;
                expect(json.ListPartResult.Bucket[0]).to.equal(bucketName);
                expect(json.ListPartResult.Key[0]).to.equal(uploadKey);
                expect(json.ListPartResult.UploadId[0]).to.equal(uploadId);
                expect(json.ListPartResult.MaxParts[0]).to.equal('4');
                expect(json.ListPartResult.Initiator[0]
                    .ID[0]).to.equal(accessKey);
                expect(json.ListPartResult.IsTruncated[0]).to.equal('true');
                expect(json.ListPartResult.PartNumberMarker).to.be.undefined;
                expect(json.ListPartResult
                    .NextPartNumberMarker[0]).to.equal('4');
                expect(json.ListPartResult.Part[2].PartNumber[0]).to.equal('3');
                expect(json.ListPartResult.Part[2].ETag[0])
                    .to.equal(sixMBObjectEtag);
                expect(json.ListPartResult.Part[2].Size[0])
                    .to.equal('6000000');
                expect(json.ListPartResult.Part).to.have.length.of(4);
                done();
            });
        });
    });

    it('should list all parts if requested max-parts ' +
    'is greater than total number of parts', (done) => {
        const listRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`,
            },
            url: `/${uploadKey}?uploadId=${uploadId}`,
            namespace: namespace,
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            query: {
                uploadId: uploadId,
                'max-parts': '6',
            }
        };

        listParts(accessKey, metastore, listRequest, (err, xml) => {
            expect(err).to.be.null;
            parseString(xml, (err, json) => {
                expect(err).to.be.null;
                expect(json.ListPartResult.Bucket[0]).to.equal(bucketName);
                expect(json.ListPartResult.Key[0]).to.equal(uploadKey);
                expect(json.ListPartResult.UploadId[0]).to.equal(uploadId);
                expect(json.ListPartResult.MaxParts[0]).to.equal('6');
                expect(json.ListPartResult.Initiator[0]
                    .ID[0]).to.equal(accessKey);
                expect(json.ListPartResult.IsTruncated[0]).to.equal('false');
                expect(json.ListPartResult.PartNumberMarker).to.be.undefined;
                expect(json.ListPartResult
                    .NextPartNumberMarker).to.be.undefined;
                expect(json.ListPartResult.Part[2].PartNumber[0]).to.equal('3');
                expect(json.ListPartResult.Part[2].ETag[0])
                    .to.equal(sixMBObjectEtag);
                expect(json.ListPartResult.Part[2].Size[0])
                    .to.equal('6000000');
                expect(json.ListPartResult.Part).to.have.length.of(5);
                done();
            });
        });
    });

    it('should only list parts after PartNumberMarker', (done) => {
        const listRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: `/${uploadKey}?uploadId=${uploadId}`,
            namespace: namespace,
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            query: {
                uploadId: uploadId,
                'part-number-marker': '2',
            }
        };

        listParts(accessKey, metastore, listRequest, (err, xml) => {
            expect(err).to.be.null;
            parseString(xml, (err, json) => {
                expect(err).to.be.null;
                expect(json.ListPartResult.Bucket[0]).to.equal(bucketName);
                expect(json.ListPartResult.Key[0]).to.equal(uploadKey);
                expect(json.ListPartResult.UploadId[0]).to.equal(uploadId);
                expect(json.ListPartResult.MaxParts[0]).to.equal('1000');
                expect(json.ListPartResult.Initiator[0]
                    .ID[0]).to.equal(accessKey);
                expect(json.ListPartResult.IsTruncated[0]).to.equal('false');
                expect(json.ListPartResult.PartNumberMarker[0]).to.equal('2');
                expect(json.ListPartResult
                    .NextPartNumberMarker).to.be.undefined;
                expect(json.ListPartResult.Part[0].PartNumber[0]).to.equal('3');
                expect(json.ListPartResult.Part[0].ETag[0])
                    .to.equal(sixMBObjectEtag);
                expect(json.ListPartResult.Part[0].Size[0])
                    .to.equal('6000000');
                expect(json.ListPartResult.Part[2].PartNumber[0]).to.equal('5');
                expect(json.ListPartResult.Part).to.have.length.of(3);
                done();
            });
        });
    });

    it('should handle a part-number-marker specified ' +
    'and a max-parts specified', (done) => {
        const listRequest = {
            lowerCaseHeaders: {
                host: `${bucketName}.s3.amazonaws.com`
            },
            url: `/${uploadKey}?uploadId=${uploadId}`,
            namespace: namespace,
            headers: {host: `${bucketName}.s3.amazonaws.com`},
            query: {
                uploadId: uploadId,
                'part-number-marker': '2',
                'max-parts': '2',
            }
        };

        listParts(accessKey, metastore, listRequest, (err, xml) => {
            expect(err).to.be.null;
            parseString(xml, (err, json) => {
                expect(err).to.be.null;
                expect(json.ListPartResult.Bucket[0]).to.equal(bucketName);
                expect(json.ListPartResult.Key[0]).to.equal(uploadKey);
                expect(json.ListPartResult.UploadId[0]).to.equal(uploadId);
                expect(json.ListPartResult.MaxParts[0]).to.equal('2');
                expect(json.ListPartResult.Initiator[0]
                    .ID[0]).to.equal(accessKey);
                expect(json.ListPartResult.IsTruncated[0]).to.equal('true');
                expect(json.ListPartResult.PartNumberMarker[0]).to.equal('2');
                expect(json.ListPartResult
                    .NextPartNumberMarker[0]).to.equal('4');
                expect(json.ListPartResult.Part[0].PartNumber[0]).to.equal('3');
                expect(json.ListPartResult.Part[0].ETag[0])
                    .to.equal(sixMBObjectEtag);
                expect(json.ListPartResult.Part[0].Size[0])
                    .to.equal('6000000');
                expect(json.ListPartResult.Part[1].PartNumber[0]).to.equal('4');
                expect(json.ListPartResult.Part).to.have.length.of(2);
                done();
            });
        });
    });
});
