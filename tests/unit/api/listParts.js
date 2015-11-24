import { expect } from 'chai';
import { parseString } from 'xml2js';

import Bucket from '../../../lib/bucket_mem';
import listParts from '../../../lib/api/listParts';

const accessKey = 'accessKey1';
const namespace = 'default';
const uploadId = '6ae3d09d-7b65-4bca-bc4f-c4695badfe41';
const bucketName = 'freshestbucket';
const uploadKey = '$makememulti';
const sixMBObjectEtag = 'f3a9fb2071d3503b703938a74eb99846';
const lastPieceEtag = '555e4cd2f9eff38109d7a3ab13995a32';

describe('List Parts API', () => {
    let metastore;

    beforeEach(() => {
        const sampleBucketInstance = new Bucket();
        sampleBucketInstance.owner = accessKey;
        sampleBucketInstance.name = bucketName;
        sampleBucketInstance.multipartObjectKeyMap = {
            "6ae3d09d-7b65-4bca-bc4f-c4695badfe41": {
                "owner": {
                    "displayName":
                        "placeholder display name for now",
                    "id": "accessKey1"
                },
                "initiator": {
                    "displayName":
                        "placeholder display name for now",
                    "id": "accessKey1"
                },
                "partLocations": [
                    null,
                    {
                        "size": "6000000",
                        "location":
                            "b005cd4088cb50c2f3c9e1d766ec9241",
                        "etag": sixMBObjectEtag,
                        "lastModified": "2015-11-20T17:28:09.599Z"
                    },
                    {
                        "size": "6000000",
                        "location":
                            "8f8b2d8aec1b99c9cea4a05c36a3b801",
                        "etag": sixMBObjectEtag,
                        "lastModified": "2015-11-20T17:30:55.897Z"
                    },
                    {
                        "size": "6000000",
                        "location":
                            "24a1a9c335c3314d8124501e3d558207",
                        "etag": sixMBObjectEtag,
                        "lastModified": "2015-11-20T17:28:55.672Z"
                    },
                    {
                        "size": "6000000",
                        "location":
                            "59daeddbc6b2f768dc26fae702cfb52e",
                        "etag": sixMBObjectEtag,
                        "lastModified": "2015-11-20T17:29:09.656Z"
                    },
                    {
                        "size": "18",
                        "location":
                            "22a3c06e0b021335c7768a079d597768",
                        "etag": lastPieceEtag,
                        "lastModified": "2015-11-20T17:30:43.707Z"
                    }
                ],
                "key": uploadKey,
                "initiated": "2015-11-20T17:27:23.017Z",
                "uploadId": uploadId,
                "x-amz-storage-class": "Standard",
                "acl": {
                    "Canned": "private",
                    "FULL_CONTROL": [],
                    "WRITE_ACP": [],
                    "READ": [],
                    "READ_ACP": []
                }
            }
        };

        metastore = {
            "users": {
                "accessKey1": {
                    "buckets": [
                        sampleBucketInstance
                    ]
                },
                "accessKey2": {
                    "buckets": []
                }
            },
            "buckets": {
                "0969df071dc0de6603230850ac138a30": sampleBucketInstance,
            }
        };
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
