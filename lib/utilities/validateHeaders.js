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
export default function validateHeaders(objectMD, headers) {
    const lastModified = new Date(objectMD['last-modified']).getTime();
    const contentMD5 = objectMD['content-md5'];
    let ifModifiedSinceTime = headers['if-modified-since'];
    let ifUnmodifiedSinceTime = headers['if-unmodified-since'];
    let ifETagMatch = headers['if-match'];
    let ifETagNoneMatch = headers['if-none-match'];
    if (ifETagMatch) {
        if (ifETagMatch.indexOf(',')) {
            ifETagMatch = ifETagMatch.split(',');
            const anyMatch = ifETagMatch.some(item =>
                item === contentMD5 || item === '*'
            );
            if (!anyMatch) {
                return { error: errors.PreconditionFailed };
            }
        } else if (ifETagMatch !== contentMD5) {
            return { error: errors.PreconditionFailed };
        }
    }
    if (ifETagNoneMatch) {
        if (ifETagNoneMatch.indexOf(',')) {
            ifETagNoneMatch = ifETagNoneMatch.split(',');
            const anyMatch = ifETagNoneMatch.some(item =>
                item === contentMD5 || item === '*'
            );
            // If both of the If-None-Match and If-Modified-Since headers
            // are present in the request and If-None-Match fails,
            // return 304 Not Modified regardless of If-Modified-Since
            if (anyMatch) {
                return { error: errors.NotModified };
            }
        } else if (ifETagNoneMatch === contentMD5) {
            return { error: errors.NotModified };
        }
    }
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
        // If-Unmodified-Since condition evaluates to false but If-Match
        // evaluated to true, then no error.
        if (lastModified > ifUnmodifiedSinceTime && !ifETagMatch) {
            return { error: errors.PreconditionFailed };
        }
    }
    return {};
}
