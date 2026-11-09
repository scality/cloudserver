const url = require('url');
const querystring = require('querystring');
const { errors } = require('arsenal');

/** parseCopySource - parse objectCopy or objectPutCopyPart copy source header
 * @param {string} apiMethod - api method
 * @param {string} copySourceHeader - 'x-amz-copy-source' request header
 * @return {object} - sourceBucket, sourceObject, sourceVersionId (as sent by
 * the client, the copy APIs decode it), parsingError
 */
function parseCopySource(apiMethod, copySourceHeader) {
    if (apiMethod !== 'objectCopy' && apiMethod !== 'objectPutCopyPart') {
        return {};
    }
    const { pathname, query } = url.parse(copySourceHeader);
    let source = querystring.unescape(pathname);
    // If client sends the source bucket/object with a leading /, remove it
    if (source[0] === '/') {
        source = source.slice(1);
    }
    const slashSeparator = source.indexOf('/');
    if (slashSeparator === -1) {
        return { parsingError: errors.InvalidArgument };
    }
    // Pull the source bucket and source object separated by /
    const sourceBucket = source.slice(0, slashSeparator);
    const sourceObject = source.slice(slashSeparator + 1);
    const sourceQuery = query ? querystring.parse(query) : undefined;
    // A repeated versionId is parsed as an array
    if (sourceQuery?.versionId !== undefined && typeof sourceQuery.versionId !== 'string') {
        return { parsingError: errors.InvalidArgument };
    }
    // Authorization compares s3:VersionId with the version ID as the client
    // sent it, like for a direct GetObject
    const sourceVersionId = sourceQuery?.versionId;

    return { sourceBucket, sourceObject, sourceVersionId };
}

module.exports = parseCopySource;
