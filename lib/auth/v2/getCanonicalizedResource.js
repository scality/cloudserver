import url from 'url';


function getCanonicalizedResource(request) {
    /*
    This variable is used to determine whether to insert
    a '?' or '&'.  Once a query parameter is added to the resourceString,
    it changes to '&' before any new query parameter is added.
    */
    let queryChar = '?';
    // If bucket specified in hostname, add to resourceString
    let resourceString = request.gotBucketNameFromHost ?
        `/${request.bucketName}` : '';
    // Add the path to the resourceString
    resourceString += url.parse(request.url).pathname;

    /*
    If request includes a specified subresource,
    add to the resourceString: (a) a '?', (b) the subresource,
    and (c) its value (if any).
    Separate multiple subresources with '&'.
    Subresources must be in alphabetical order.
    */

    // Specified subresources:
    const subresources = [
        'acl',
        'lifecycle',
        'location',
        'logging',
        'notification',
        'partNumber',
        'policy',
        'requestPayment',
        'torrent',
        'uploadId',
        'uploads',
        'versionId',
        'versioning',
        'versions',
        'website',
    ];

    /*
    If the request includes parameters in the query string,
    that override the headers, include
    them in the resourceString
    along with their values.
    AWS is ambiguous about format.  Used alphabetical order.
    */
    const overridingParams = [
        'response-cache-control',
        'response-content-disposition',
        'response-content-encoding',
        'response-content-language',
        'response-content-type',
        'response-expires',
    ];

    // Check which specified subresources are present in query string,
    // build array with them
    const query = request.query;
    const presentSubresources = Object.keys(query).filter((val) => {
        return subresources.indexOf(val) !== -1;
    });
    // Sort the array and add the subresources and their value (if any)
    // to the resourceString
    presentSubresources.sort();
    resourceString = presentSubresources.reduce((prev, current) => {
        const ch = (query[current] !== '' ? '=' : '');
        const ret = `${prev}${queryChar}${current}${ch}${query[current]}`;
        queryChar = '&';
        return ret;
    }, resourceString);
    // Add the overriding parameters to our resourceString
    resourceString = overridingParams.reduce((prev, current) => {
        if (query[current]) {
            const ret = `${prev}${queryChar}${current}=${query[current]}`;
            queryChar = '&';
            return ret;
        }
        return prev;
    }, resourceString);

    /*
    Per AWS, the delete query string parameter must be included when
    you create the CanonicalizedResource for a multi-object Delete request.
    Unclear what this means for a single item delete request.
    */
    if (request.query.delete) {
        // Addresses adding '?' instead of '&' if no other params added.
        resourceString += `${queryChar}delete=${query.delete}`;
    }
    return resourceString;
}

export default getCanonicalizedResource;
