const { ListObjectsV2Command,
    ListObjectVersionsCommand,
    DeleteObjectsCommand } = require('@aws-sdk/client-s3');
const assert = require('assert');

async function _deleteVersionList(s3Client, versionList, bucket) {
    if (!versionList || versionList.length === 0) {
        return;
    }

    const params = {
        Bucket: bucket,
        Delete: {
            Objects: versionList.map(version => ({
                Key: version.Key,
                VersionId: version.VersionId
            }))
        }
    };

    const command = new DeleteObjectsCommand(params);
    await s3Client.send(command);
}

const testUtils = {};

testUtils.runIfMongo = process.env.S3METADATA === 'mongodb' ?
    describe : describe.skip;

testUtils.runAndCheckSearch = async (s3Client, bucketName, encodedSearch, listVersions, testResult) => {
    try {
        if (!encodedSearch) {
            throw new Error('encodedSearch is empty or undefined');
        }

        let searchQuery;
        const decodedSearch = decodeURIComponent(encodedSearch);
        const isValidSql = decodedSearch.includes('=') && (decodedSearch.includes("'") || decodedSearch.includes('"'));
        
        if (!isValidSql && !decodedSearch.includes(' ')) {
            searchQuery = encodeURIComponent(`key='${decodedSearch}'`);
        } else {
            searchQuery = encodedSearch;
        }

        if (listVersions) {
            const command = new ListObjectVersionsCommand({
                Bucket: bucketName
            });

            // Modify the request to include the custom search and versions parameters
            command.middlewareStack.add(next => async args => {
                // eslint-disable-next-line no-param-reassign
                args.request.query = {
                    ...args.request.query,
                    search: searchQuery,
                    versions: ''
                };
                // eslint-disable-next-line no-param-reassign
                args.request.path = `/${bucketName}`;
                return next(args);
            }, {
                step: 'build',
                priority: 'high' // Run before signing middleware
            });

            const res = await s3Client.send(command);
            // eslint-disable-next-line no-console
            console.log('ListObjectVersions response:', JSON.stringify(res, null, 2));

            if (testResult) {
                assert.notStrictEqual(res.Versions?.[0]?.VersionId, undefined, 'VersionId should be defined');
                if (Array.isArray(testResult)) {
                    assert.strictEqual(res.Versions?.length, testResult.length, 'Versions length mismatch');
                    for (let i = 0; i < testResult.length; i++) {
                        assert.strictEqual(res.Versions[i].Key, testResult[i], `Key mismatch at index ${i}`);
                    }
                } else {
                    assert(res.Versions?.[0], 'should be Versions listed');
                    assert.strictEqual(res.Versions[0].Key, testResult, `Key does not match testResult: ${testResult}`);
                    assert.strictEqual(res.Versions.length, 1, 'Expected exactly one version');
                }
            } else {
                assert.strictEqual(res.Versions, undefined, 'Expected no versions');
            }
        } else {
            const command = new ListObjectsV2Command({
                Bucket: bucketName
            });

            // Modify the request to include the custom search parameter
            command.middlewareStack.add(next => async args => {
                // eslint-disable-next-line no-param-reassign
                args.request.query = {
                    ...args.request.query,
                    search: searchQuery
                };
                // eslint-disable-next-line no-param-reassign
                args.request.path = `/${bucketName}`;
                return next(args);
            }, {
                step: 'build',
                priority: 'high' // Run before signing middleware
            });

            const res = await s3Client.send(command);
            if (testResult) {
                assert(res.Contents?.[0], 'should be Contents listed');
                assert.strictEqual(res.Contents[0].Key, testResult, `Key does not match testResult: ${testResult}`);
                assert.strictEqual(res.Contents.length, 1, 'Expected exactly one object');
                // eslint-disable-next-line no-console
                console.log('ListObjectsV2 response we heeeeeere');
            } else {
                assert.strictEqual(res.Contents, undefined, 'Expected no objects');
            }
        }
    } catch (err) {
        if (testResult && typeof testResult === 'object' && testResult.code) {
            assert.strictEqual(err.name, testResult.code, `Expected error code 
                ${testResult.code}, got ${err.name}`);
            assert.strictEqual(err.message, testResult.message, 
                `Expected error message ${testResult.message}, got ${err.message}`);
        } else {
            throw err; // Re-throw unexpected errors
        }
    }
};

testUtils.removeAllVersions = async (s3Client, bucket) => {
    try {
        let isTruncated = true;
        let nextKeyMarker = null;
        let nextVersionIdMarker = null;

        while (isTruncated) {
            const params = {
                Bucket: bucket,
                ...(nextKeyMarker && { KeyMarker: nextKeyMarker }),
                ...(nextVersionIdMarker && { VersionIdMarker: nextVersionIdMarker })
            };

            const listCommand = new ListObjectVersionsCommand(params);
            const data = await s3Client.send(listCommand);
            // eslint-disable-next-line no-console
            console.log('ListObjectVersions response in removeAllVersions:', data);

            // Delete DeleteMarkers if they exist
            if (data.DeleteMarkers?.length) {
                await _deleteVersionList(s3Client, data.DeleteMarkers, bucket);
            }

            // Delete Versions if they exist
            if (data.Versions?.length) {
                await _deleteVersionList(s3Client, data.Versions, bucket);
            }

            isTruncated = data.IsTruncated || false;
            nextKeyMarker = data.NextKeyMarker;
            nextVersionIdMarker = data.NextVersionIdMarker;
        }
    } catch (err) {
        // eslint-disable-next-line no-console
        console.error('Error in removeAllVersions:', err);
        throw err;
    }
};

module.exports = testUtils;
