import { errors } from 'arsenal';

/**
 * Checks 'if-modified-since', 'if-unmodified-since', 'if-match' or
 * 'if-none-match' headers if included in request against last-modified
 * date of object and/or ETag.
 * @param {object} objectMD - object's metadata
 * @param {object} headers - contains headers from request object
 * @return {object} object with error as key and arsenal error as value or
 * empty object if no error
 */
function validateHeaders(objectMD, headers) {
    const lastModified = new Date(objectMD['last-modified']).getTime();
    const contentMD5 = objectMD['content-md5'];
    let ifModifiedSinceTime = headers['if-modified-since'];
    let ifUnmodifiedSinceTime = headers['if-unmodified-since'];
    const ifETagMatch = headers['if-match'];
    const ifETagNoneMatch = headers['if-none-match'];
    if (ifModifiedSinceTime) {
        ifModifiedSinceTime = new Date(ifModifiedSinceTime);
        ifModifiedSinceTime = ifModifiedSinceTime.getTime();
        if (isNaN(ifModifiedSinceTime)) {
            return { error: errors.InvalidArgument };
        }
        if (lastModified < ifModifiedSinceTime) {
            return { error: errors.NotModified };
        }
    }
    if (ifUnmodifiedSinceTime) {
        ifUnmodifiedSinceTime = new Date(ifUnmodifiedSinceTime);
        ifUnmodifiedSinceTime = ifUnmodifiedSinceTime.getTime();
        if (isNaN(ifUnmodifiedSinceTime)) {
            return { error: errors.InvalidArgument };
        }
        if (lastModified > ifUnmodifiedSinceTime) {
            return { error: errors.PreconditionFailed };
        }
    }
    if (ifETagMatch) {
        if (ifETagMatch !== contentMD5) {
            return { error: errors.PreconditionFailed };
        }
    }
    if (ifETagNoneMatch) {
        if (ifETagNoneMatch === contentMD5) {
            return { error: errors.NotModified };
        }
    }
    return {};
}

export default validateHeaders;
