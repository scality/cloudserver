const assert = require('assert');
const HttpRequestAuthV4 = require('../utils/HttpRequestAuthV4');

const bucket = 'xxx';
const objectKey = 'key';
const objData = Buffer.alloc(1, 'a');

const authCredentials = {
    accessKey: 'accessKey1',
    secretKey: 'verySecretKey1',
};

const itSkipIfAWS = process.env.AWS_ON_AIR ? it.skip : it;

describe('Test x-amz-checksums', () => {
    const algos = [
        { name: 'CRC32', objDataDigest: '6Le+Qw==', validWrong: 'AAAAAA==' },
        { name: 'CRC32C', objDataDigest: 'wdBDMA==', validWrong: 'AAAAAA==' },
        { name: 'CRC64NVME', objDataDigest: 'jC+ERbTL/Dw=', validWrong: 'AAAAAAAAAAA=' },
        { name: 'SHA1', objDataDigest: 'hvfkN/qlp/zhXR3cuerq6jd2Z7g=', validWrong: 'AAAAAAAAAAAAAAAAAAAAAAAAAAA=' },
        {
            name: 'SHA256',
            objDataDigest: 'ypeBEsobvcr6wjGzmiPcTaeG7/gUfE5yuYB3ha/uSLs=',
            validWrong: 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=',
        },
    ];
    // CompleteMultipartUpload intentionally not listed here: its
    // x-amz-checksum-<algo> header is the expected final-object checksum,
    // not a body digest, so it's not part of the buffered-body validator
    // path tested below.
    const methods = [
        {
            Name: 'DeleteObjects',
            Query: 'delete',
            Key: '',
            HTTPMethod: 'POST',
        },
        {
            Name: 'PutBucketACL',
            Query: 'acl',
            Key: '',
            HTTPMethod: 'PUT',
        },
        {
            Name: 'PutBucketACL',
            Query: 'cors',
            Key: '',
            HTTPMethod: 'PUT',
        },
        {
            Name: 'PutBucketEncryption',
            Query: 'encryption',
            Key: '',
            HTTPMethod: 'PUT',
        },
        {
            Name: 'PutBucketLifecycke',
            Query: 'lifecycle',
            Key: '',
            HTTPMethod: 'PUT',
        },
        {
            Name: 'PutBucketLogging',
            Query: 'logging',
            Key: '',
            HTTPMethod: 'PUT',
        },
        {
            Name: 'PutBucketNotification',
            Query: 'notification',
            Key: '',
            HTTPMethod: 'PUT',
        },
        {
            Name: 'PutBucketPolicy',
            Query: 'policy',
            Key: '',
            HTTPMethod: 'PUT',
        },
        {
            Name: 'PutBucketReplication',
            Query: 'replication',
            Key: '',
            HTTPMethod: 'PUT',
        },
        {
            Name: 'PutBucketTagging',
            Query: 'tagging',
            Key: '',
            HTTPMethod: 'PUT',
        },
        {
            Name: 'PutBucketVersioning',
            Query: 'versioning',
            Key: '',
            HTTPMethod: 'PUT',
        },
        {
            Name: 'PutBucketWebsite',
            Query: 'website',
            Key: '',
            HTTPMethod: 'PUT',
        },
        {
            Name: 'PutObjectACL',
            Query: 'acl',
            Key: objectKey,
            HTTPMethod: 'PUT',
        },
        {
            Name: 'PutObjectLegalHold',
            Query: 'legal-hold',
            Key: objectKey,
            HTTPMethod: 'PUT',
        },
        {
            Name: 'PutObjectLockConfiguration',
            Query: 'object-lock',
            Key: '',
            HTTPMethod: 'PUT',
        },
        {
            Name: 'PutObjectRetention',
            Query: 'retention',
            Key: objectKey,
            HTTPMethod: 'PUT',
        },
        {
            Name: 'PutObjectTagging',
            Query: 'tagging',
            Key: objectKey,
            HTTPMethod: 'PUT',
        },
        {
            Name: 'RestoreObject',
            Query: 'restore',
            Key: objectKey,
            HTTPMethod: 'POST',
        },
    ];

    const doTest = (headers, method, resHTTPStatus, errMsgs, done) => {
        const url = `http://localhost:8000/${bucket}/${method.Key}?${method.Query}`;
        const req = new HttpRequestAuthV4(
            url,
            Object.assign(
                {
                    method: method.HTTPMethod,
                    headers: {
                        'x-amz-content-sha256': 'ypeBEsobvcr6wjGzmiPcTaeG7/gUfE5yuYB3ha/uSLs=',
                        'content-length': objData.length,
                        ...headers,
                    },
                },
                authCredentials,
            ),
            res => {
                let data = '';
                res.on('data', chunk => {
                    data += chunk;
                });
                res.on('end', () => {
                    assert.strictEqual(res.statusCode, resHTTPStatus);
                    for (const errMsg of errMsgs) {
                        assert(data.includes(errMsg), `missing ${errMsg} in "${data}"`);
                    }
                    done();
                });
            },
        );

        req.on('error', err => {
            assert.ifError(err);
        });

        req.once('drain', () => {
            req.end();
        });

        req.write(objData, err => {
            assert.ifError(err);
            req.end();
        });
    };

    for (const algo of algos) {
        for (const method of methods) {
            itSkipIfAWS(
                `${method.Name} should respond BadDigest ` + `with invalid x-amz-checksum-${algo.name.toLowerCase()}`,
                done => {
                    const headers = {
                        [`x-amz-checksum-${algo.name.toLowerCase()}`]: algo.validWrong,
                    };
                    doTest(headers, method, 400, ['BadDigest'], done);
                },
            );
        }
    }

    itSkipIfAWS('should respond InvalidRequest with multiple x-amz-checksum-', done => {
        const headers = {
            [`x-amz-checksum-${algos[0].name.toLowerCase()}`]: algos[0].objDataDigest,
            [`x-amz-checksum-${algos[1].name.toLowerCase()}`]: algos[1].objDataDigest,
        };
        doTest(
            headers,
            methods[0],
            400,
            ['InvalidRequest', 'Expecting a single x-amz-checksum- header. Multiple checksum Types are not allowed.'],
            done,
        );
    });

    itSkipIfAWS('should respond InvalidRequest with invalid x-amz-checksum- algorithm', done => {
        const headers = {
            ['x-amz-checksum-BAD']: algos[0].objDataDigest,
        };
        doTest(
            headers,
            methods[0],
            400,
            ['InvalidRequest', 'The algorithm type you specified in x-amz-checksum- header is invalid'],
            done,
        );
    });

    itSkipIfAWS('should respond InvalidRequest if the value of x-amz-sdk-checksum-algorithm is invalid', done => {
        const headers = {
            'x-amz-sdk-checksum-algorithm': 'BAD',
            [`x-amz-checksum-${algos[0].name.toLowerCase()}`]: algos[0].objDataDigest,
        };
        doTest(
            headers,
            methods[0],
            400,
            ['InvalidRequest', 'Value for x-amz-sdk-checksum-algorithm header is invalid.'],
            done,
        );
    });

    itSkipIfAWS('should respond InvalidRequest with if invalid x-amz-checksum- value', done => {
        const headers = {
            ['x-amz-checksum-sha256']: 'BAD',
        };
        doTest(
            headers,
            methods[0],
            400,
            ['InvalidRequest', 'Value for x-amz-checksum-sha256 header is invalid.'],
            done,
        );
    });

    itSkipIfAWS(
        'should respond InvalidRequest with if missing x-amz-checksum- for x-amz-sdk-checksum-algorithm ',
        done => {
            const headers = {
                'x-amz-sdk-checksum-algorithm': 'SHA1',
            };
            doTest(
                headers,
                methods[0],
                400,
                [
                    'InvalidRequest',
                    'x-amz-sdk-checksum-algorithm specified, but no corresponding x-amz-checksum-* ' +
                        'or x-amz-trailer headers were found.',
                ],
                done,
            );
        },
    );

    for (const algo of algos) {
        for (const method of methods) {
            itSkipIfAWS(
                `${method.Name} should not respond BadDigest if ` +
                    `x-amz-checksum-${algo.name.toLowerCase()} is correct`,
                done => {
                    const url = `http://localhost:8000/${bucket}/${method.Key}?${method.Query}`;
                    const req = new HttpRequestAuthV4(
                        url,
                        Object.assign(
                            {
                                method: method.HTTPMethod,
                                headers: {
                                    'x-amz-content-sha256': 'ypeBEsobvcr6wjGzmiPcTaeG7/gUfE5yuYB3ha/uSLs=',
                                    'content-length': objData.length,
                                    'x-amz-sdk-checksum-algorithm': algo.name,
                                    [`x-amz-checksum-${algo.name.toLowerCase()}`]: algo.objDataDigest,
                                },
                            },
                            authCredentials,
                        ),
                        res => {
                            let data = '';
                            res.on('data', chunk => {
                                data += chunk;
                            });
                            res.on('end', () => {
                                assert(!data.includes('BadDigest'));
                                assert(!data.includes('InvalidDigest'));
                                assert(!data.includes('x-amz-checksum'));
                                assert(!data.includes('x-amz-sdk-checksum-algorithm'));
                                assert(!data.includes('did not match the calculated checksum'));
                                done();
                            });
                        },
                    );

                    req.on('error', err => {
                        assert.ifError(err);
                    });

                    req.once('drain', () => {
                        req.end();
                    });

                    req.write(objData, err => {
                        assert.ifError(err);
                        req.end();
                    });
                },
            );
        }
    }
});
