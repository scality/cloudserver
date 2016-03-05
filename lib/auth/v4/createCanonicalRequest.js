import awsURIencode from './awsURIencode';
/**
 * createCanonicalRequest - creates V4 canonical request
 * @param {object} params - contains pHttpVerb (request type),
 * pResource (parsed from URL), pQuery (request query),
 * pHeaders (request headers), pSignedHeaders (signed headers from request),
 * payloadChecksum (from request)
 * @returns {string} - canonicalRequest
 */
function createCanonicalRequest(params) {
    const { pHttpVerb, pResource, pQuery, pHeaders, pSignedHeaders,
        payloadChecksum } = params;
    const canonicalURI = awsURIencode(pResource, false);

    // canonical query string
    const queryParams = Object.keys(pQuery).map(key => {
        const value = pQuery[key] ? awsURIencode(pQuery[key]) : '';
        return {
            qParam: awsURIencode(key),
            value,
        };
    });

    queryParams.sort((a, b) => { return a.qParam.localeCompare(b.qParam);});
    const sortedQueryParams = queryParams.map(item => {
        return `${item.qParam}=${item.value}`;
    });
    const canonicalQueryStr = sortedQueryParams.join('&');

    // signed headers
    const signedHeadersList = pSignedHeaders.split(';');
    signedHeadersList.sort((a, b) => {
        return a.localeCompare(b);
    });
    const signedHeaders = signedHeadersList.join(';');

    // canonical headers
    const canonicalHeadersList = signedHeadersList.filter(signedHeader => {
        return pHeaders[signedHeader];
    }).map(signedHeader => {
        return `${signedHeader}:${pHeaders[signedHeader]}\n`;
    });

    const canonicalHeaders = canonicalHeadersList.join('');

    const canonicalRequest = `${pHttpVerb}\n${canonicalURI}\n` +
        `${canonicalQueryStr}\n${canonicalHeaders}\n` +
        `${signedHeaders}\n${payloadChecksum}`;

    return canonicalRequest;
}

export default createCanonicalRequest;
