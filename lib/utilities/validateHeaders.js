const { errors } = require('arsenal');


function checkEtagMatch(ifETagMatch, contentMD5) {
    const res = { present: false, error: null };
    if (ifETagMatch) {
        res.present = true;
        if (ifETagMatch.indexOf(',')) {
            const items = ifETagMatch.split(',');
            const anyMatch = items.some(item =>
                item === contentMD5 || item === '*' ||
                item === `"${contentMD5}"`
            );
            if (!anyMatch) {
                res.error = errors.PreconditionFailed;
            }
        } else if (ifETagMatch !== contentMD5) {
            res.error = errors.PreconditionFailed;
        }
    }
    return res;
}

function checkEtagNoneMatch(ifETagNoneMatch, contentMD5) {
    const res = { present: false, error: null };
    if (ifETagNoneMatch) {
        res.present = true;
        if (ifETagNoneMatch.indexOf(',')) {
            const items = ifETagNoneMatch.split(',');
            const anyMatch = items.some(item =>
                item === contentMD5 || item === '*' ||
                item === `"${contentMD5}"`
            );
            if (anyMatch) {
                res.error = errors.NotModified;
            }
        } else if (ifETagNoneMatch === contentMD5) {
            res.error = errors.NotModified;
        }
    }
    return res;
}

function checkModifiedSince(ifModifiedSinceTime, lastModified) {
    const res = { present: false, error: null };
    if (ifModifiedSinceTime) {
        res.present = true;
        const checkWith = (new Date(ifModifiedSinceTime)).getTime();
        if (isNaN(checkWith)) {
            res.error = errors.InvalidArgument;
        } else if (lastModified <= checkWith) {
            res.error = errors.NotModified;
        }
    }
    return res;
}

function checkUnmodifiedSince(ifUnmodifiedSinceTime, lastModified) {
    const res = { present: false, error: null };
    if (ifUnmodifiedSinceTime) {
        res.present = true;
        const checkWith = (new Date(ifUnmodifiedSinceTime)).getTime();
        if (isNaN(checkWith)) {
            res.error = errors.InvalidArgument;
        } else if (lastModified > checkWith) {
            res.error = errors.PreconditionFailed;
        }
    }
    return res;
}

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
    let lastModified = new Date(objectMD['last-modified']);
    lastModified.setMilliseconds(0);
    lastModified = lastModified.getTime();
    const contentMD5 = objectMD['content-md5'];
    const ifMatchHeader = headers['if-match'] ||
        headers['x-amz-copy-source-if-match'];
    const ifNoneMatchHeader = headers['if-none-match'] ||
        headers['x-amz-copy-source-if-none-match'];
    const ifModifiedSinceHeader = headers['if-modified-since'] ||
        headers['x-amz-copy-source-if-modified-since'];
    const ifUnmodifiedSinceHeader = headers['if-unmodified-since'] ||
        headers['x-amz-copy-source-if-unmodified-since'];
    const etagMatchRes = checkEtagMatch(ifMatchHeader, contentMD5);
    const etagNoneMatchRes = checkEtagNoneMatch(ifNoneMatchHeader, contentMD5);
    const modifiedSinceRes = checkModifiedSince(ifModifiedSinceHeader,
        lastModified);
    const unmodifiedSinceRes = checkUnmodifiedSince(ifUnmodifiedSinceHeader,
        lastModified);
    // If-Unmodified-Since condition evaluates to false and If-Match
    // is not present, then return the error. Otherwise, If-Unmodified-Since is
    // silent when If-Match match, and when If-Match does not match, it's the
    // same error, so each case are covered.
    if (!etagMatchRes.present && unmodifiedSinceRes.error) {
        return unmodifiedSinceRes;
    }
    if (etagMatchRes.present && etagMatchRes.error) {
        return etagMatchRes;
    }
    if (etagNoneMatchRes.present && etagNoneMatchRes.error) {
        return etagNoneMatchRes;
    }
    if (modifiedSinceRes.present && modifiedSinceRes.error) {
        return modifiedSinceRes;
    }
    return {};
}

module.exports = validateHeaders;
