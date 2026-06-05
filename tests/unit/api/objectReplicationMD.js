const assert = require('assert');
const async = require('async');
const crypto = require('crypto');
const { promisify } = require('util');

const BucketInfo = require('arsenal').models.BucketInfo;

const { cleanup, DummyRequestLogger, makeAuthInfo, TaggingConfigTester } = require('../helpers');
const constants = require('../../../constants');
const { metadata } = require('arsenal').storage.metadata.inMemory.metadata;
const DummyRequest = require('../DummyRequest');
const { objectDelete } = require('../../../lib/api/objectDelete');
const objectPut = require('../../../lib/api/objectPut');
const objectCopy = require('../../../lib/api/objectCopy');
const completeMultipartUpload = require('../../../lib/api/completeMultipartUpload');
const objectPutACL = require('../../../lib/api/objectPutACL');
const objectPutTagging = require('../../../lib/api/objectPutTagging');
const objectDeleteTagging = require('../../../lib/api/objectDeleteTagging');
const { config } = require('../../../lib/Config');

const log = new DummyRequestLogger();
const authInfo = makeAuthInfo('accessKey1');
const ownerID = authInfo.getCanonicalID();
const authInfoLifecycleService = makeAuthInfo('lifecycleKey1');
const namespace = 'default';
const bucketName = 'source-bucket';
const mpuShadowBucket = `${constants.mpuBucketPrefix}${bucketName}`;
const bucketARN = `arn:aws:s3:::${bucketName}`;
const storageClassType = 'zenko';
const keyA = 'key-A';
const keyB = 'key-B';

const deleteReq = new DummyRequest({
    bucketName,
    namespace,
    objectKey: keyA,
    headers: {},
    url: `/${bucketName}/${keyA}`,
});

const objectACLReq = {
    bucketName,
    namespace,
    objectKey: keyA,
    headers: {
        'x-amz-grant-read': `id=${ownerID}`,
        'x-amz-grant-read-acp': `id=${ownerID}`,
    },
    url: `/${bucketName}/${keyA}?acl`,
    query: { acl: '' },
    actionImplicitDenies: false,
};

// Get an object request with the given key.
function getObjectPutReq(key, hasContent) {
    const bodyContent = hasContent ? 'body content' : '';
    return new DummyRequest(
        {
            bucketName,
            namespace,
            objectKey: key,
            headers: {},
            url: `/${bucketName}/${key}`,
        },
        Buffer.from(bodyContent, 'utf8'),
    );
}

const taggingPutReq = new TaggingConfigTester().createObjectTaggingRequest('PUT', bucketName, keyA);
const taggingDeleteReq = new TaggingConfigTester().createObjectTaggingRequest('DELETE', bucketName, keyA);

const emptyReplicationMD = {
    status: '',
    backends: [],
    content: [],
};

// Check that the object key has the expected replication information.
// Normalizes via JSON round-trip to drop undefined-valued keys so that
// expectations don't need to know whether the MD path went through a
// metadata read (which JSON-serializes and drops undefined fields).
function checkObjectReplicationInfo(key, expected) {
    const objectMD = metadata.keyMaps.get(bucketName).get(key);
    const actual = JSON.parse(JSON.stringify(objectMD.replicationInfo));
    assert.deepStrictEqual(actual, expected);
}

// Put the object key and check the replication information.
function putObjectAndCheckMD(key, expected, cb) {
    return objectPut(authInfo, getObjectPutReq(key, true), undefined, log, err => {
        if (err) {
            return cb(err);
        }
        checkObjectReplicationInfo(key, expected);
        return cb();
    });
}

// Create the bucket in metadata.
function createBucket() {
    metadata.buckets.set(bucketName, new BucketInfo(bucketName, ownerID, '', ''));
    metadata.keyMaps.set(bucketName, new Map());
}

// Create the bucket in metadata with versioning and a replication config.
function createBucketWithReplication(hasStorageClass) {
    createBucket();
    const config = {
        role: 'arn:aws:iam::account-id:role/src-resource,' + 'arn:aws:iam::account-id:role/dest-resource',
        destination: 'arn:aws:s3:::source-bucket',
        rules: [
            {
                prefix: keyA,
                enabled: true,
                id: 'test-id',
            },
        ],
    };
    if (hasStorageClass) {
        config.rules[0].storageClass = storageClassType;
    }
    Object.assign(metadata.buckets.get(bucketName), {
        _versioningConfiguration: { status: 'Enabled' },
        _replicationConfiguration: config,
    });
}

// Create the shadow bucket in metadata for MPUs with a recent model number.
function createShadowBucket(key, uploadId) {
    const overviewKey = `overview${constants.splitter}` + `${key}${constants.splitter}${uploadId}`;
    metadata.buckets.set(mpuShadowBucket, new BucketInfo(mpuShadowBucket, ownerID, '', ''));
    // Set modelVersion to use the most recent splitter.
    Object.assign(metadata.buckets.get(mpuShadowBucket), {
        _mdBucketModelVersion: 5,
    });
    metadata.keyMaps.set(mpuShadowBucket, new Map());
    metadata.keyMaps.get(mpuShadowBucket).set(overviewKey, new Map());
    Object.assign(metadata.keyMaps.get(mpuShadowBucket).get(overviewKey), {
        id: uploadId,
        eventualStorageBucket: bucketName,
        initiator: {
            DisplayName: 'accessKey1displayName',
            ID: ownerID,
        },
        key,
        uploadId,
    });
}

// Initiate an MPU, put a part with the given body, and complete the MPU.
function putMPU(key, body, cb) {
    const uploadId = '9a0364b9e99bb480dd25e1f0284c8555';
    createShadowBucket(key, uploadId);
    const partBody = Buffer.from(body, 'utf8');
    const md5Hash = crypto.createHash('md5').update(partBody);
    const calculatedHash = md5Hash.digest('hex');
    const partKey = `${uploadId}${constants.splitter}00001`;
    const obj = {
        partLocations: [
            {
                key: 1,
                dataStoreName: 'scality-internal-mem',
                dataStoreETag: `1:${calculatedHash}`,
            },
        ],
        key: partKey,
    };
    obj['content-md5'] = calculatedHash;
    obj['content-length'] = body.length;
    metadata.keyMaps.get(mpuShadowBucket).set(partKey, new Map());
    const partMap = metadata.keyMaps.get(mpuShadowBucket).get(partKey);
    Object.assign(partMap, obj);
    const postBody =
        '<CompleteMultipartUpload>' +
        '<Part>' +
        '<PartNumber>1</PartNumber>' +
        `<ETag>"${calculatedHash}"</ETag>` +
        '</Part>' +
        '</CompleteMultipartUpload>';
    const req = {
        bucketName,
        namespace,
        objectKey: key,
        parsedHost: 's3.amazonaws.com',
        url: `/${key}?uploadId=${uploadId}`,
        headers: { host: `${bucketName}.s3.amazonaws.com` },
        query: { uploadId },
        post: postBody,
        actionImplicitDenies: false,
    };
    return completeMultipartUpload(authInfo, req, log, cb);
}

// Copy an object where replication does not apply.
function copyObject(sourceObjectKey, copyObjectKey, hasContent, cb) {
    const req = getObjectPutReq(sourceObjectKey, hasContent);
    return objectPut(authInfo, req, undefined, log, err => {
        if (err) {
            return cb(err);
        }
        const req = new DummyRequest({
            bucketName,
            namespace,
            objectKey: copyObjectKey,
            headers: {},
            url: `/${bucketName}/${sourceObjectKey}`,
        });
        return objectCopy(authInfo, req, bucketName, sourceObjectKey, undefined, log, cb);
    });
}

describe('Replication object MD without bucket replication config', () => {
    beforeEach(() => {
        cleanup();
        createBucket();
    });

    afterEach(() => cleanup());

    it('should not update object metadata', done => putObjectAndCheckMD(keyA, emptyReplicationMD, done));

    it('should not update object metadata if putting object ACL', done =>
        async.series(
            [
                next => putObjectAndCheckMD(keyA, emptyReplicationMD, next),
                next => objectPutACL(authInfo, objectACLReq, log, next),
            ],
            err => {
                if (err) {
                    return done(err);
                }
                checkObjectReplicationInfo(keyA, emptyReplicationMD);
                return done();
            },
        ));

    describe('Object tagging', () => {
        beforeEach(done =>
            async.series(
                [
                    next => putObjectAndCheckMD(keyA, emptyReplicationMD, next),
                    next => objectPutTagging(authInfo, taggingPutReq, log, next),
                ],
                err => done(err),
            ),
        );

        it('should not update object metadata if putting tag', done => {
            checkObjectReplicationInfo(keyA, emptyReplicationMD);
            return done();
        });

        it('should not update object metadata if deleting tag', done =>
            async.series(
                [
                    // Put a new version to update replication MD content array.
                    next => putObjectAndCheckMD(keyA, emptyReplicationMD, next),
                    next => objectDeleteTagging(authInfo, taggingDeleteReq, log, next),
                ],
                err => {
                    if (err) {
                        return done(err);
                    }
                    checkObjectReplicationInfo(keyA, emptyReplicationMD);
                    return done();
                },
            ));

        it('should not update object metadata if completing MPU', done =>
            putMPU(keyA, 'content', err => {
                if (err) {
                    return done(err);
                }
                checkObjectReplicationInfo(keyA, emptyReplicationMD);
                return done();
            }));

        it('should not update object metadata if copying object', done =>
            copyObject(keyB, keyA, true, err => {
                if (err) {
                    return done(err);
                }
                checkObjectReplicationInfo(keyA, emptyReplicationMD);
                return done();
            }));
    });
});

[true, false].forEach(hasStorageClass => {
    describe(
        'Replication object MD with bucket replication config ' +
            `${hasStorageClass ? 'with' : 'without'} storage class`,
        () => {
            const replicationMD = {
                status: 'PENDING',
                backends: [
                    {
                        site: 'zenko',
                        status: 'PENDING',
                        dataStoreVersionId: '',
                        destination: bucketARN,
                        role: 'arn:aws:iam::account-id:role/dest-resource',
                    },
                ],
                content: ['DATA', 'METADATA'],
                role: 'arn:aws:iam::account-id:role/src-resource',
            };
            const newReplicationMD = replicationMD;
            const replicateMetadataOnly = Object.assign({}, newReplicationMD, { content: ['METADATA'] });

            beforeEach(() => {
                cleanup();
                createBucketWithReplication(hasStorageClass);
            });

            afterEach(() => {
                cleanup();
                delete config.locationConstraints['zenko'];
            });

            it('should update metadata when replication config prefix matches ' + 'an object key', done =>
                putObjectAndCheckMD(keyA, newReplicationMD, done),
            );

            it('should update metadata when replication config prefix matches ' + 'the start of an object key', done =>
                putObjectAndCheckMD(`${keyA}abc`, newReplicationMD, done),
            );

            it(
                'should not update metadata when replication config prefix does ' +
                    'not match the start of an object key',
                done => putObjectAndCheckMD(`abc${keyA}`, emptyReplicationMD, done),
            );

            it('should not update metadata when replication config prefix does ' + 'not apply', done =>
                putObjectAndCheckMD(keyB, emptyReplicationMD, done),
            );

            it("should update status to 'PENDING' if putting a new version", done =>
                putObjectAndCheckMD(keyA, newReplicationMD, err => {
                    if (err) {
                        return done(err);
                    }
                    const objectMD = metadata.keyMaps.get(bucketName).get(keyA);
                    // Update metadata to a status after replication has occurred.
                    objectMD.replicationInfo.status = 'COMPLETED';
                    return putObjectAndCheckMD(keyA, newReplicationMD, done);
                }));

            it("should update status to 'PENDING' and content to '['METADATA']' " + 'if putting 0 byte object', done =>
                objectPut(authInfo, getObjectPutReq(keyA, false), undefined, log, err => {
                    if (err) {
                        return done(err);
                    }
                    checkObjectReplicationInfo(keyA, replicateMetadataOnly);
                    return done();
                }),
            );

            it('should update metadata if putting object ACL and CRR replication', done => {
                // Set 'zenko' as a typical CRR location (i.e. no type)
                config.locationConstraints['zenko'] = {
                    ...config.locationConstraints['zenko'],
                    type: '',
                };

                async.series(
                    [
                        next => putObjectAndCheckMD(keyA, newReplicationMD, next),
                        next => {
                            const objectMD = metadata.keyMaps.get(bucketName).get(keyA);
                            // Update metadata to a status after replication has occurred.
                            objectMD.replicationInfo.status = 'COMPLETED';
                            objectPutACL(authInfo, objectACLReq, log, next);
                        },
                    ],
                    err => {
                        if (err) {
                            return done(err);
                        }
                        checkObjectReplicationInfo(keyA, replicateMetadataOnly);
                        return done();
                    },
                );
            });

            it('should not update metadata if putting object ACL and cloud replication', done => {
                // Set 'zenko' as a typical cloud location (i.e.  type)
                config.locationConstraints['zenko'] = {
                    ...config.locationConstraints['zenko'],
                    type: 'aws_s3',
                };

                const replicationMD = {
                    ...newReplicationMD,
                    backends: [
                        {
                            site: 'zenko',
                            status: 'PENDING',
                            dataStoreVersionId: '',
                        },
                    ],
                };

                let completedReplicationInfo;
                async.series(
                    [
                        next => putObjectAndCheckMD(keyA, replicationMD, next),
                        next => {
                            const objectMD = metadata.keyMaps.get(bucketName).get(keyA);
                            // Update metadata to a status after replication has occurred.
                            objectMD.replicationInfo.status = 'COMPLETED';
                            completedReplicationInfo = JSON.parse(JSON.stringify(objectMD.replicationInfo));
                            objectPutACL(authInfo, objectACLReq, log, next);
                        },
                    ],
                    err => {
                        if (err) {
                            return done(err);
                        }
                        checkObjectReplicationInfo(keyA, completedReplicationInfo);
                        return done();
                    },
                );
            });

            it('should update metadata if putting a delete marker', done =>
                async.series(
                    [
                        next =>
                            putObjectAndCheckMD(keyA, newReplicationMD, err => {
                                if (err) {
                                    return next(err);
                                }
                                const objectMD = metadata.keyMaps.get(bucketName).get(keyA);
                                // Set metadata to a status after replication has occurred.
                                objectMD.replicationInfo.status = 'COMPLETED';
                                return next();
                            }),
                        next => objectDelete(authInfo, deleteReq, log, next),
                    ],
                    err => {
                        if (err) {
                            return done(err);
                        }
                        const objectMD = metadata.keyMaps.get(bucketName).get(keyA);
                        assert.strictEqual(objectMD.isDeleteMarker, true);
                        checkObjectReplicationInfo(keyA, replicateMetadataOnly);
                        return done();
                    },
                ));

            it('should not update metadata if putting a delete marker owned by ' + 'Lifecycle service account', done =>
                async.series(
                    [
                        next => putObjectAndCheckMD(keyA, newReplicationMD, next),
                        next => objectDelete(authInfoLifecycleService, deleteReq, log, next),
                    ],
                    err => {
                        if (err) {
                            return done(err);
                        }
                        const objectMD = metadata.keyMaps.get(bucketName).get(keyA);
                        assert.strictEqual(objectMD.isDeleteMarker, true);
                        checkObjectReplicationInfo(keyA, emptyReplicationMD);
                        return done();
                    },
                ),
            );

            describe('Object tagging', () => {
                beforeEach(done =>
                    async.series(
                        [
                            next => putObjectAndCheckMD(keyA, newReplicationMD, next),
                            next => objectPutTagging(authInfo, taggingPutReq, log, next),
                        ],
                        err => done(err),
                    ),
                );

                it("should update status to 'PENDING' and content to " + "'['METADATA']'if putting tag", done => {
                    checkObjectReplicationInfo(keyA, replicateMetadataOnly);
                    return done();
                });

                it("should update status to 'PENDING' and content to " + "'['METADATA']' if deleting tag", done =>
                    async.series(
                        [
                            // Put a new version to update replication MD content array.
                            next => putObjectAndCheckMD(keyA, newReplicationMD, next),
                            next => objectDeleteTagging(authInfo, taggingDeleteReq, log, next),
                        ],
                        err => {
                            if (err) {
                                return done(err);
                            }
                            checkObjectReplicationInfo(keyA, replicateMetadataOnly);
                            return done();
                        },
                    ),
                );
            });

            describe('Complete MPU', () => {
                it(
                    "should update status to 'PENDING' and content to " + "'['DATA, METADATA']' if completing MPU",
                    done =>
                        putMPU(keyA, 'content', err => {
                            if (err) {
                                return done(err);
                            }
                            checkObjectReplicationInfo(keyA, newReplicationMD);
                            return done();
                        }),
                );

                it(
                    "should update status to 'PENDING' and content to " +
                        "'['METADATA']' if completing MPU with 0 bytes",
                    done =>
                        putMPU(keyA, '', err => {
                            if (err) {
                                return done(err);
                            }
                            checkObjectReplicationInfo(keyA, replicateMetadataOnly);
                            return done();
                        }),
                );

                it('should not update replicationInfo if key does not apply', done =>
                    putMPU(keyB, 'content', err => {
                        if (err) {
                            return done(err);
                        }
                        checkObjectReplicationInfo(keyB, emptyReplicationMD);
                        return done();
                    }));
            });

            describe('Object copy', () => {
                it(
                    "should update status to 'PENDING' and content to " + "'['DATA, METADATA']' if copying object",
                    done =>
                        copyObject(keyB, keyA, true, err => {
                            if (err) {
                                return done(err);
                            }
                            checkObjectReplicationInfo(keyA, newReplicationMD);
                            return done();
                        }),
                );

                it(
                    "should update status to 'PENDING' and content to " +
                        "'['METADATA']' if copying object with 0 bytes",
                    done =>
                        copyObject(keyB, keyA, false, err => {
                            if (err) {
                                return done(err);
                            }
                            checkObjectReplicationInfo(keyA, replicateMetadataOnly);
                            return done();
                        }),
                );

                it('should not update replicationInfo if key does not apply', done => {
                    const copyKey = `foo-${keyA}`;
                    return copyObject(keyB, copyKey, true, err => {
                        if (err) {
                            return done(err);
                        }
                        checkObjectReplicationInfo(copyKey, emptyReplicationMD);
                        return done();
                    });
                });
            });

            ['awsbackend', 'azurebackend', 'gcpbackend', 'awsbackend,azurebackend'].forEach(backend => {
                const backends = backend.split(',').map(site => ({
                    site,
                    status: 'PENDING',
                    dataStoreVersionId: '',
                }));
                describe('Object metadata replicationInfo for cloud backends', () => {
                    const expectedReplicationInfo = {
                        status: 'PENDING',
                        backends,
                        content: ['DATA', 'METADATA'],
                        role: 'arn:aws:iam::account-id:role/resource',
                    };

                    // Expected for a metadata-only replication operation (for
                    // example, putting object tags).
                    const expectedReplicationInfoMD = Object.assign({}, expectedReplicationInfo, {
                        content: ['METADATA'],
                    });

                    beforeEach(() =>
                        // We have already created the bucket, so update the
                        // replication configuration to include a location
                        // constraint for the storage class.
                        Object.assign(metadata.buckets.get(bucketName), {
                            _replicationConfiguration: {
                                role: 'arn:aws:iam::account-id:role/resource',
                                destination: 'arn:aws:s3:::destination-bucket',
                                rules: [
                                    {
                                        prefix: keyA,
                                        enabled: true,
                                        id: 'test-id',
                                        storageClass: backend,
                                    },
                                ],
                            },
                        }),
                    );

                    it('should update on a put object request', done =>
                        putObjectAndCheckMD(keyA, expectedReplicationInfo, done));

                    it('should update on a complete MPU object request', done =>
                        putMPU(keyA, 'content', err => {
                            if (err) {
                                return done(err);
                            }
                            const expected = Object.assign({}, expectedReplicationInfo, {
                                content: ['DATA', 'METADATA', 'MPU'],
                            });
                            checkObjectReplicationInfo(keyA, expected);
                            return done();
                        }));

                    it('should update on a copy object request', done =>
                        copyObject(keyB, keyA, true, err => {
                            if (err) {
                                return done(err);
                            }
                            checkObjectReplicationInfo(keyA, expectedReplicationInfo);
                            return done();
                        }));

                    it('should update on a put object ACL request', done => {
                        let completedReplicationInfo;
                        async.series(
                            [
                                next => putObjectAndCheckMD(keyA, expectedReplicationInfo, next),
                                next => {
                                    const objectMD = metadata.keyMaps.get(bucketName).get(keyA);
                                    // Update metadata to a status after replication
                                    // has occurred.
                                    objectMD.replicationInfo.status = 'COMPLETED';
                                    completedReplicationInfo = JSON.parse(JSON.stringify(objectMD.replicationInfo));
                                    objectPutACL(authInfo, objectACLReq, log, next);
                                },
                            ],
                            err => {
                                if (err) {
                                    return done(err);
                                }
                                checkObjectReplicationInfo(keyA, completedReplicationInfo);
                                return done();
                            },
                        );
                    });

                    it('should update on a put object tagging request', done =>
                        async.series(
                            [
                                next => putObjectAndCheckMD(keyA, expectedReplicationInfo, next),
                                next => objectPutTagging(authInfo, taggingPutReq, log, next),
                            ],
                            err => {
                                if (err) {
                                    return done(err);
                                }
                                const expected = Object.assign({}, expectedReplicationInfo, {
                                    content: ['METADATA', 'PUT_TAGGING'],
                                });
                                checkObjectReplicationInfo(keyA, expected);
                                return done();
                            },
                        ));

                    it('should update on a delete tagging request', done =>
                        async.series(
                            [
                                next => putObjectAndCheckMD(keyA, expectedReplicationInfo, next),
                                next => objectDeleteTagging(authInfo, taggingDeleteReq, log, next),
                            ],
                            err => {
                                if (err) {
                                    return done(err);
                                }
                                const expected = Object.assign({}, expectedReplicationInfo, {
                                    content: ['METADATA', 'DELETE_TAGGING'],
                                });
                                checkObjectReplicationInfo(keyA, expected);
                                return done();
                            },
                        ));

                    it('should update when putting a delete marker', done =>
                        async.series(
                            [
                                next =>
                                    putObjectAndCheckMD(keyA, expectedReplicationInfo, err => {
                                        if (err) {
                                            return next(err);
                                        }
                                        // Update metadata to a status indicating that
                                        // replication has occurred for the object.
                                        metadata.keyMaps.get(bucketName).get(keyA).replicationInfo.status = 'COMPLETED';
                                        return next();
                                    }),
                                next => objectDelete(authInfo, deleteReq, log, next),
                            ],
                            err => {
                                if (err) {
                                    return done(err);
                                }
                                // Is it, in fact, a delete marker?
                                assert(metadata.keyMaps.get(bucketName).get(keyA).isDeleteMarker);
                                checkObjectReplicationInfo(keyA, expectedReplicationInfoMD);
                                return done();
                            },
                        ));
                });
            });
        },
    );
});

describe('Replication object MD with CRR and cloud destinations on the same object', () => {
    const crrSite = 'crr-site';
    const cloudSite = 'awsbackend';
    const crrRule = {
        id: 'rule-crr',
        prefix: keyA,
        enabled: true,
        priority: 1,
        storageClass: crrSite,
        destination: 'arn:aws:s3:::crr-bucket',
    };
    const cloudRule = {
        id: 'rule-cloud',
        prefix: keyA,
        enabled: true,
        priority: 2,
        storageClass: cloudSite,
        destination: 'arn:aws:s3:::aws-bucket',
    };

    function setupBucket(rules) {
        cleanup();
        createBucket();
        config.locationConstraints[crrSite] = { type: '' };
        config.locationConstraints[cloudSite] = { type: 'aws_s3' };
        Object.assign(metadata.buckets.get(bucketName), {
            _versioningConfiguration: { status: 'Enabled' },
            _replicationConfiguration: {
                role: 'arn:aws:iam::account-id:role/src-role,' + 'arn:aws:iam::account-id:role/dst-role',
                rules,
            },
        });
    }

    function completeAllBackends() {
        const objectMD = metadata.keyMaps.get(bucketName).get(keyA);
        objectMD.replicationInfo.status = 'COMPLETED';
        objectMD.replicationInfo.backends.forEach(b => {
            // eslint-disable-next-line no-param-reassign
            b.status = 'COMPLETED';
        });
    }

    afterEach(() => {
        cleanup();
        delete config.locationConstraints[crrSite];
        delete config.locationConstraints[cloudSite];
    });

    it(
        'should reset only the CRR backend to PENDING on putObjectACL, ' +
            'preserving the completed status of the cloud backend',
        done => {
            setupBucket([crrRule, cloudRule]);
            async.series(
                [
                    next => objectPut(authInfo, getObjectPutReq(keyA, true), undefined, log, next),
                    next => {
                        completeAllBackends();
                        return objectPutACL(authInfo, objectACLReq, log, next);
                    },
                ],
                err => {
                    if (err) {
                        return done(err);
                    }
                    const objectMD = metadata.keyMaps.get(bucketName).get(keyA);
                    const crrBackend = objectMD.replicationInfo.backends.find(b => b.site === crrSite);
                    const cloudBackend = objectMD.replicationInfo.backends.find(b => b.site === cloudSite);
                    // CRR backend is re-kicked: status reset to PENDING with the
                    // resolved destination role stamped on the entry.
                    assert.strictEqual(crrBackend.status, 'PENDING');
                    assert.strictEqual(crrBackend.role, 'arn:aws:iam::account-id:role/dst-role');
                    assert.strictEqual(crrBackend.destination, 'arn:aws:s3:::crr-bucket');
                    // Cloud backend is left alone: no ACL replication for cloud,
                    // and no resolved role/destination on the entry.
                    assert.strictEqual(cloudBackend.status, 'COMPLETED');
                    assert.strictEqual(cloudBackend.role, undefined);
                    assert.strictEqual(cloudBackend.destination, undefined);
                    return done();
                },
            );
        },
    );

    it('should not touch replicationInfo when no CRR backend is present', done => {
        setupBucket([cloudRule]);
        async.series(
            [
                next => objectPut(authInfo, getObjectPutReq(keyA, true), undefined, log, next),
                next => {
                    completeAllBackends();
                    return objectPutACL(authInfo, objectACLReq, log, next);
                },
            ],
            err => {
                if (err) {
                    return done(err);
                }
                const objectMD = metadata.keyMaps.get(bucketName).get(keyA);
                // Status untouched because nothing to ACL-replicate.
                assert.strictEqual(objectMD.replicationInfo.status, 'COMPLETED');
                objectMD.replicationInfo.backends.forEach(b => {
                    assert.strictEqual(b.status, 'COMPLETED');
                });
                return done();
            },
        );
    });

    it('should add a newly configured CRR destination to backends on ' + 'putObjectACL', done => {
        setupBucket([cloudRule]);
        async.series(
            [
                next => objectPut(authInfo, getObjectPutReq(keyA, true), undefined, log, next),
                next => {
                    completeAllBackends();
                    // Operator adds a CRR destination after the object was
                    // already replicated to cloud.
                    metadata.buckets.get(bucketName)._replicationConfiguration.rules.unshift(crrRule);
                    return objectPutACL(authInfo, objectACLReq, log, next);
                },
            ],
            err => {
                if (err) {
                    return done(err);
                }
                const objectMD = metadata.keyMaps.get(bucketName).get(keyA);
                const crrBackend = objectMD.replicationInfo.backends.find(b => b.site === crrSite);
                const cloudBackend = objectMD.replicationInfo.backends.find(b => b.site === cloudSite);
                assert.ok(crrBackend, 'new CRR backend should be added');
                assert.strictEqual(crrBackend.status, 'PENDING');
                assert.strictEqual(cloudBackend.status, 'COMPLETED');
                return done();
            },
        );
    });

    it('should not add a newly configured cloud destination to backends on putObjectACL', async () => {
        setupBucket([crrRule]);
        await promisify(objectPut)(authInfo, getObjectPutReq(keyA, true), undefined, log);
        completeAllBackends();

        metadata.buckets.get(bucketName)._replicationConfiguration.rules.push(cloudRule);
        await promisify(objectPutACL)(authInfo, objectACLReq, log);

        const objectMD = metadata.keyMaps.get(bucketName).get(keyA);
        const crrBackend = objectMD.replicationInfo.backends.find(b => b.site === crrSite);
        const cloudBackend = objectMD.replicationInfo.backends.find(b => b.site === cloudSite);

        assert.strictEqual(crrBackend.status, 'PENDING');
        assert.strictEqual(cloudBackend, undefined, 'new cloud backend should not be added');
    });
});
