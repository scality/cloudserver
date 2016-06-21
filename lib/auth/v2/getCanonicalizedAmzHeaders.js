function getCanonicalizedAmzHeaders(headers) {
    /*
    Iterate through headers and pull any headers that are x-amz headers.
    Need to include 'x-amz-date' here even though AWS docs
    ambiguous on this.
    */
    const amzHeaders = Object.keys(headers)
        .filter(val => val.substr(0, 6) === 'x-amz-')
        .map(val => [val.trim(), headers[val].trim()]);
    /*
    AWS docs state that duplicate headers should be combined
    in the same header with values concatenated with
    a comma separation.
    Node combines duplicate headers and concatenates the values
    with a comma AND SPACE separation.
    Could replace all occurrences of ', ' with ',' but this
    would remove spaces that might be desired
    (for instance, in date header).
    Opted to proceed without this parsing since it does not appear
    that the AWS clients use duplicate headers.
    */

    // If there are no amz headers, just return an empty string
    if (amzHeaders.length === 0) {
        return '';
    }


    // Sort the amz headers by key (first item in tuple)
    amzHeaders.sort((a, b) => {
        if (a[0] > b[0]) {
            return 1;
        }
        return -1;
    });
    // Build headerString
    return amzHeaders.reduce(
        (headerStr, current) => `${headerStr}${current[0]}:${current[1]}\n`,
        '');
}

export default getCanonicalizedAmzHeaders;
