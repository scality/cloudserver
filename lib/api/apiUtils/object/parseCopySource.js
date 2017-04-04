import { errors } from 'arsenal';

import url from 'url';
import querystring from 'querystring';
import { decodeVersionId } from './versioning';

/** parseCopySource - parse objectCopy or objectPutCopyPart copy source header
 * @param {string} apiMethod - api method
 * @param {string} copySourceHeader - 'x-amz-copy-source' request header
 * @return {object} - sourceBucket, sourceObject, sourceVersionId, parsingError
 */
export default function parseCopySource(apiMethod, copySourceHeader) {
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
    const sourceVersionId =
        decodeVersionId(query ? querystring.parse(query) : undefined);
    if (sourceVersionId instanceof Error) {
        const err = sourceVersionId;
        return { parsingError: err };
    }

    return { sourceBucket, sourceObject, sourceVersionId };
}
