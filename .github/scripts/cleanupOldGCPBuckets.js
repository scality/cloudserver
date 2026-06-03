/* eslint-disable no-console */
'use strict';

const {
    S3Client,
    ListBucketsCommand,
    ListObjectsV2Command,
    DeleteObjectCommand,
    DeleteBucketCommand,
    ListMultipartUploadsCommand,
    AbortMultipartUploadCommand,
} = require('@aws-sdk/client-s3');

const GCP_ENDPOINT = 'https://storage.googleapis.com';
const BUCKET_PREFIX = 'cldsrvci-';
const ONE_WEEK_MS = 7 * 24 * 60 * 60 * 1000;

function buildClient() {
    const accessKeyId = process.env.AWS_GCP_BACKEND_ACCESS_KEY;
    const secretAccessKey = process.env.AWS_GCP_BACKEND_SECRET_KEY;

    if (!accessKeyId || !secretAccessKey) {
        console.error(
            'Missing required environment variables: ' + 'AWS_GCP_BACKEND_ACCESS_KEY and AWS_GCP_BACKEND_SECRET_KEY',
        );
        process.exit(1);
    }

    const client = new S3Client({
        endpoint: GCP_ENDPOINT,
        region: 'us-east-1',
        credentials: { accessKeyId, secretAccessKey },
        forcePathStyle: true,
        disableS3ExpressSessionAuth: true,
        requestChecksumCalculation: 'WHEN_REQUIRED',
        responseChecksumValidation: 'WHEN_REQUIRED',
    });

    // GCP's S3-compatible API rejects the x-id query parameter that newer
    // versions of @aws-sdk/client-s3 append to every request URL.
    client.middlewareStack.add(
        next => async args => {
            // eslint-disable-next-line no-param-reassign
            delete args.request.query['x-id'];
            return next(args);
        },
        { step: 'build', name: 'removeXIdParam', priority: 'low' },
    );

    return client;
}

async function abortMultipartUploads(client, bucketName) {
    let uploadIdMarker;
    let keyMarker;

    do {
        const res = await client.send(
            new ListMultipartUploadsCommand({
                Bucket: bucketName,
                UploadIdMarker: uploadIdMarker,
                KeyMarker: keyMarker,
            }),
        );

        for (const upload of res.Uploads || []) {
            console.log(`  Aborting MPU: ${upload.Key} (${upload.UploadId})`);
            await client.send(
                new AbortMultipartUploadCommand({
                    Bucket: bucketName,
                    Key: upload.Key,
                    UploadId: upload.UploadId,
                }),
            );
        }

        uploadIdMarker = res.NextUploadIdMarker;
        keyMarker = res.NextKeyMarker;
    } while (uploadIdMarker);
}

async function deleteAllObjects(client, bucketName) {
    let continuationToken;

    do {
        const res = await client.send(
            new ListObjectsV2Command({
                Bucket: bucketName,
                ContinuationToken: continuationToken,
            }),
        );

        const objects = res.Contents || [];
        if (objects.length > 0) {
            console.log(`  Deleting ${objects.length} object(s)...`);
            for (const obj of objects) {
                await client.send(
                    new DeleteObjectCommand({
                        Bucket: bucketName,
                        Key: obj.Key,
                    }),
                );
            }
        }

        continuationToken = res.NextContinuationToken;
    } while (continuationToken);
}

async function cleanupBucket(client, bucketName) {
    console.log(`Cleaning up bucket: ${bucketName}`);
    try {
        await abortMultipartUploads(client, bucketName);
        await deleteAllObjects(client, bucketName);
        await client.send(new DeleteBucketCommand({ Bucket: bucketName }));
        console.log(`Deleted bucket: ${bucketName}`);
    } catch (err) {
        console.error(`Failed to delete bucket ${bucketName}: ${err.message}`);
    }
}

async function main() {
    const client = buildClient();

    console.log('Listing GCP buckets...');
    const { Buckets = [] } = await client.send(new ListBucketsCommand({}));

    const now = Date.now();
    const stale = Buckets.filter(
        b => b.Name.startsWith(BUCKET_PREFIX) && now - new Date(b.CreationDate).getTime() > ONE_WEEK_MS,
    );

    if (stale.length === 0) {
        console.log('No stale GCP CI buckets found.');
        return;
    }

    console.log(`Found ${stale.length} stale GCP CI bucket(s) to clean up: ${stale.map(b => b.Name).join(', ')}`);

    for (const bucket of stale) {
        await cleanupBucket(client, bucket.Name);
    }

    console.log('GCP CI bucket cleanup complete.');
}

main().catch(err => {
    console.error('GCP cleanup script failed:', err);
    process.exit(1);
});
