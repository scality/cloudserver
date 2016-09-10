export default function pushMetrics(err, log, utapi, action, resource,
    contentLength, prevContentLen) {
    if (!err) {
        const reqUid = log.getSerializedUids();
        switch (action) {
        case 'bucketPutACL':
            utapi.pushMetricPutBucketAcl(reqUid, resource);
            break;
        case 'bucketPut':
            utapi.pushMetricCreateBucket(reqUid, resource);
            break;
        case 'objectPutPart':
            utapi.pushMetricUploadPart(reqUid, resource, contentLength);
            break;
        case 'objectPutACL':
            utapi.pushMetricPutObjectAcl(reqUid, resource);
            break;
        case 'objectPut':
            utapi.pushMetricPutObject(reqUid, resource, contentLength,
                prevContentLen);
            break;
        case 'bucketGetACL':
            utapi.pushMetricGetBucketAcl(reqUid, resource);
            break;
        case 'listMultipartUploads':
            utapi.pushMetricListBucketMultipartUploads(reqUid, resource);
            break;
        case 'bucketGet':
            utapi.pushMetricListBucket(reqUid, resource);
            break;
        case 'objectGetACL':
            utapi.pushMetricGetObjectAcl(reqUid, resource);
            break;
        case 'listParts':
            utapi.pushMetricListMultipartUploadParts(reqUid, resource);
            break;
        case 'objectGet':
            utapi.pushMetricGetObject(reqUid, resource, contentLength);
            break;
        case 'bucketHead':
            utapi.pushMetricHeadBucket(reqUid, resource);
            break;
        case 'objectHead':
            utapi.pushMetricHeadObject(reqUid, resource);
            break;
        case 'bucketDelete':
            utapi.pushMetricDeleteBucket(reqUid, resource);
            break;
        case 'objectDelete':
            utapi.pushMetricDeleteObject(reqUid, resource, contentLength);
            break;
        case 'multipartDelete':
            utapi.pushMetricAbortMultipartUpload(reqUid, resource);
            break;
        case 'initiateMultipartUpload':
            utapi.pushMetricInitiateMultipartUpload(reqUid, resource);
            break;
        case 'completeMultipartUpload':
            utapi.pushMetricCompleteMultipartUpload(reqUid, resource);
            break;
        default: throw new Error('Unkown action for push metrics');
        }
    }
}
