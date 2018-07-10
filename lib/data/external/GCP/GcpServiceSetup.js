/**
 * This file contains the private methods for the GCP service to form/sign
 * Google Cloud Storage requests.
 *
 * It uses a modified version of the S3 service private methods to form/sign
 * requests compatible with the Google Cloud Storage XML api.
 */

const AWS = require('aws-sdk');
const GcpSigner = require('./GcpSigner');

module.exports = {
    /**
     * GCP compatible request signer
     * @return {AWS.Signer} AWS Signer
     */
    getSignerClass() {
        return GcpSigner;
    },

    /**
     * set service region
     * @return {undefined}
     */
    validateService() {
        if (!this.config.region) {
            this.config.region = 'us-east-1';
        }
    },

    /**
     * setup listeners for building requests
     * @param {AWS.Request} request - AWS request object
     * @return {undefined}
     */
    setupRequestListeners(request) {
        request.addListener('validate', this.validateBucketEndpoint);
        request.addListener('build', this.addContentType);
        request.addListener('build', this.populateURI);
        request.addListener('build', this.computeContentMd5);
    },

    /**
     * validate that when bucket endpoitn flag is set, root level apis are
     * inaccessible
     * @param {AWS.Request} req - AWS request object
     * @returns {undefined}
     * @api private
     */
    validateBucketEndpoint(req) {
        if (!req.params.Bucket && req.service.config.s3BucketEndpoint) {
            const msg =
                'Cannot send requests to root API with `s3BucketEndpoint` set.';
            throw AWS.util.error(new Error(),
                { code: 'ConfigError', message: msg });
        }
    },

    /**
     * S3 prefers dns-compatible bucket names to be moved from the uri path
     * to the hostname as a sub-domain. This is not possible, even for
     * dns-compat buckets when using SSL and the bucket name contains a dot.
     * The ssl wildcard certificate is only 1-level deep.
     * @param {AWS.Request} req - AWS request object
     * @returns {undefined}
     *
     * @api private
     */
    populateURI(req) {
        const httpRequest = req.httpRequest;
        const b = req.params.Bucket;
        const service = req.service;
        const endpoint = httpRequest.endpoint;

        if (b) {
            if (!service.pathStyleBucketName(b)) {
                if (!service.config.s3BucketEndpoint) {
                    endpoint.hostname = `${b}.${endpoint.hostname}`;
                }
                const port = endpoint.port;
                if (port !== 80 && port !== 443) {
                    endpoint.host = `${endpoint.hostname}:${endpoint.port}`;
                } else {
                    endpoint.host = endpoint.hostname;
                }
                // needed for signing the request
                httpRequest.virtualHostedBucket = b;
                service.removeVirtualHostedBucketFromPath(req);
            }
        }
    },

    /**
     * Takes the bucket name out of the path if bucket is virtual-hosted
     * @param {AWS.Request} req - AWS request object
     * @returns {undefined}
     *
     * @api private
     */
    removeVirtualHostedBucketFromPath(req) {
        const httpRequest = req.httpRequest;
        const bucket = httpRequest.virtualHostedBucket;
        if (bucket && httpRequest.path) {
            httpRequest.path =
                httpRequest.path.replace(new RegExp(`/${bucket}`), '');
            if (httpRequest.path[0] !== '/') {
                httpRequest.path = `/${httpRequest.path}`;
            }
        }
    },

    /**
     * Adds a default content type if none is supplied.
     * @param {AWS.Request} req - AWS request object
     * @returns {undefined}
     *
     * @api private
     */
    addContentType(req) {
        const httpRequest = req.httpRequest;
        if (httpRequest.method === 'GET' || httpRequest.method === 'HEAD') {
            // Content-Type is not set in GET/HEAD requests
            delete httpRequest.headers['Content-Type'];
            return;
        }

        // always have a Content-Type
        if (!httpRequest.headers['Content-Type']) {
            httpRequest.headers['Content-Type'] = 'application/octet-stream';
        }

        const contentType = httpRequest.headers['Content-Type'];
        if (AWS.util.isBrowser()) {
            if (typeof httpRequest.body === 'string' &&
                !contentType.match(/;\s*charset=/)) {
                const charset = '; charset=UTF-8';
                httpRequest.headers['Content-Type'] += charset;
            } else {
                const replaceFn = (_, prefix, charsetName) =>
                    prefix + charsetName.toUpperCase();

                httpRequest.headers['Content-Type'] =
                    contentType.replace(/(;\s*charset=)(.+)$/, replaceFn);
            }
        }
    },

    computableChecksumOperations: {
        putBucketCors: true,
        putBucketLifecycle: true,
        putBucketLifecycleConfiguration: true,
        putBucketTagging: true,
        deleteObjects: true,
        putBucketReplication: true,
    },

    /**
     * Checks whether checksums should be computed for the request.
     * If the request requires checksums to be computed, this will always
     * return true, otherwise it depends on whether
     * {AWS.Config.computeChecksums} is set.
     * @param {AWS.Request} req - the request to check against
     * @return {Boolean} whether to compute checksums for a request.
     *
     * @api private
     */
    willComputeChecksums(req) {
        if (this.computableChecksumOperations[req.operation]) return true;
        if (!this.config.computeChecksums) return false;

        // TODO: compute checksums for Stream objects
        if (!AWS.util.Buffer.isBuffer(req.httpRequest.body) &&
                typeof req.httpRequest.body !== 'string') {
            return false;
        }
        return false;
    },

    /**
     * A listener that computes the Content-MD5 and sets it in the header.
     * @param {AWS.Request} req - AWS request object
     * @returns {undefined}
     *
     * @see AWS.S3.willComputeChecksums
     * @api private
     */
    computeContentMd5(req) {
        if (req.service.willComputeChecksums(req)) {
            const md5 = AWS.util.crypto.md5(req.httpRequest.body, 'base64');
            // eslint-disable-next-line no-param-reassign
            req.httpRequest.headers['Content-MD5'] = md5;
        }
    },

    /**
     * Returns true if the bucket name should be left in the URI path for
     * a request to S3.    This function takes into account the current
     * endpoint protocol (e.g. http or https).
     * @param {string} bucketName - bucket name
     * @returns {Boolean} whether request should use path style
     *
     * @api private
     */
    pathStyleBucketName(bucketName) {
        // user can force path style requests via the configuration
        if (this.config.s3ForcePathStyle) return true;
        if (this.config.s3BucketEndpoint) return false;

        if (this.dnsCompatibleBucketName(bucketName)) {
            return this.config.sslEnabled && bucketName.match(/\./);
        }
        return true; // not dns compatible names must always use path style
    },

    /**
     * Returns true if the bucket name is DNS compatible. Buckets created
     * outside of the classic region MUST be DNS compatible.
     * @param {string} bucketName - bucket name
     * @returns {Boolean} whether bucket name is dns compatible
     *
     * @api private
     */
    dnsCompatibleBucketName(bucketName) {
        const b = bucketName;
        const domain = new RegExp(/^[a-z0-9][a-z0-9\.\-]{1,61}[a-z0-9]$/);
        const ipAddress = new RegExp(/(\d+\.){3}\d+/);
        const dots = new RegExp(/\.\./);
        return b.match(domain) && !b.match(ipAddress) && !b.match(dots);
    },
};
