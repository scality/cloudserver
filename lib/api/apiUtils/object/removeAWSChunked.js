/**
 * Checks for V4 streaming value 'aws-chunked' and removes it if present in
 * Content-Encoding to be compatible with AWS behavior. See:
 * http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html
 * @param {string} sourceHeader - Content-Encoding header from request headers
 * @return {string} new value w. 'aws-chunked'/'aws-chunked,' substring removed
 */
export default function removeAWSChunked(sourceHeader) {
    if (sourceHeader === undefined) {
        return undefined;
    }
    return sourceHeader.replace(/aws-chunked,?/, '');
}
