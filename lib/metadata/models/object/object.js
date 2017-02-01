
// Version 2 changes the format of the location property
const modelVersion = 2;

/**
 * Class to manage metadata object for regular s3 objects
 */
export default class ObjectMD {

    /**
     * @constructor
     *
     * @param {number} version - Version of the metadata model
     */
    constructor(version) {
        const now = new Date().toJSON();
        this.data = {
            'md-model-version': version || modelVersion,
            'owner-display-name': '',
            'owner-id': '',
            'cache-control': '',
            'content-disposition': '',
            'content-encoding': '',
            'expires': '',
            'content-length': 0,
            'content-type': '',
            'last-modified': now,
            'content-md5': '',
            // simple/no version. will expand once object versioning is
            // introduced
            'x-amz-version-id': 'null',
            'x-amz-server-version-id': '',
            // TODO: Handle this as a utility function for all object puts
            // similar to normalizing request but after checkAuth so
            // string to sign is not impacted.  This is GH Issue#89.
            'x-amz-storage-class': 'STANDARD',
            'x-amz-server-side-encryption': '',
            'x-amz-server-side-encryption-aws-kms-key-id': '',
            'x-amz-server-side-encryption-customer-algorithm': '',
            'acl': {
                Canned: 'private',
                FULL_CONTROL: [],
                WRITE_ACP: [],
                READ: [],
                READ_ACP: [],
            },
            'key': '',
            'location': [],
        };
    }

    /**
     * Returns metadata model version
     *
     * @return {number} Metadata model version
     */
    getModelVersion() {
        return this.data['md-model-version'];
    }

    /**
     * Set owner display name
     *
     * @param {string} displayName - Owner display name
     * @return {ObjectMD} itself
     */
    setOwnerDisplayName(displayName) {
        this.data['owner-display-name'] = displayName;
        return this;
    }

    /**
     * Returns owner display name
     *
     * @return {string} Onwer display name
     */
    getOwnerDisplayName() {
        return this.data['owner-display-name'];
    }

    /**
     * Set owner id
     *
     * @param {string} id - Owner id
     * @return {ObjectMD} itself
     */
    setOwnerId(id) {
        this.data['owner-id'] = id;
        return this;
    }

    /**
     * Returns owner id
     *
     * @return {string} owner id
     */
    getOwnerId() {
        return this.data['owner-id'];
    }

    /**
     * Set cache control
     *
     * @param {string} cacheControl - Cache control
     * @return {ObjectMD} itself
     */
    setCacheControl(cacheControl) {
        this.data['cache-control'] = cacheControl;
        return this;
    }

    /**
     * Returns cache control
     *
     * @return {string} Cache control
     */
    getCacheControl() {
        return this.data['cache-control'];
    }

    /**
     * Set content disposition
     *
     * @param {string} contentDisposition - Content disposition
     * @return {ObjectMD} itself
     */
    setContentDisposition(contentDisposition) {
        this.data['content-disposition'] = contentDisposition;
        return this;
    }

    /**
     * Returns content disposition
     *
     * @return {string} Content disposition
     */
    getContentDisposition() {
        return this.data['content-disposition'];
    }

    /**
     * Set content encoding
     *
     * @param {string} contentEncoding - Content encoding
     * @return {ObjectMD} itself
     */
    setContentEncoding(contentEncoding) {
        this.data['content-encoding'] = contentEncoding;
        return this;
    }

    /**
     * Returns content encoding
     *
     * @return {string} Content encoding
     */
    getContentEncoding() {
        return this.data['content-encoding'];
    }

    /**
     * Set expiration date
     *
     * @param {string} expires - Expiration date
     * @return {ObjectMD} itself
     */
    setExpires(expires) {
        this.data.expires = expires;
        return this;
    }

    /**
     * Returns expiration date
     *
     * @return {string} Expiration date
     */
    getExpires() {
        return this.data.expires;
    }

    /**
     * Set content length
     *
     * @param {number} contentLength - Content length
     * @return {ObjectMD} itself
     */
    setContentLength(contentLength) {
        this.data['content-length'] = contentLength;
        return this;
    }

    /**
     * Returns content length
     *
     * @return {number} Content length
     */
    getContentLength() {
        return this.data['content-length'];
    }

    /**
     * Set content type
     *
     * @param {string} contentType - Content type
     * @return {ObjectMD} itself
     */
    setContentType(contentType) {
        this.data['content-type'] = contentType;
        return this;
    }

    /**
     * Returns content type
     *
     * @return {string} Content type
     */
    getContentType() {
        return this.data['content-type'];
    }

    /**
     * Set last modified date
     *
     * @param {string} lastModified - Last modified date
     * @return {ObjectMD} itself
     */
    setLastModified(lastModified) {
        this.data['last-modified'] = lastModified;
        return this;
    }

    /**
     * Returns last modified date
     *
     * @return {string} Last modified date
     */
    getLastModified() {
        return this.data['last-modified'];
    }

    /**
     * Set content md5 hash
     *
     * @param {string} contentMd5 - Content md5 hash
     * @return {ObjectMD} itself
     */
    setContentMd5(contentMd5) {
        this.data['content-md5'] = contentMd5;
        return this;
    }

    /**
     * Returns content md5 hash
     *
     * @return {string} content md5 hash
     */
    getContentMd5() {
        return this.data['content-md5'];
    }

    /**
     * Set version id
     *
     * @param {string} versionId - Version id
     * @return {ObjectMD} itself
     */
    setAmzVersionId(versionId) {
        this.data['x-amz-version-id'] = versionId;
        return this;
    }

    /**
     * Returns version id
     *
     * @return {string} Version id
     */
    getAmzVersionId() {
        return this.data['x-amz-version-id'];
    }

    /**
     * Set server version id
     *
     * @param {string} versionId - server version id
     * @return {ObjectMD} itself
     */
    setAmzServerVersionId(versionId) {
        this.data['x-amz-server-version-id'] = versionId;
        return this;
    }

    /**
     * Returns server version id
     *
     * @return {string} server version id
     */
    getAmzServerVersionId() {
        return this.data['x-amz-server-version-id'];
    }

    /**
     * Set storage class
     *
     * @param {string} storageClass - Storage class
     * @return {ObjectMD} itself
     */
    setAmzStorageClass(storageClass) {
        this.data['x-amz-storage-class'] = storageClass;
        return this;
    }

    /**
     * Returns storage class
     *
     * @return {string} Storage class
     */
    getAmzStorageClass() {
        return this.data['x-amz-storage-class'];
    }

    /**
     * Set server side encryption
     *
     * @param {string} serverSideEncryption - Server side encryption
     * @return {ObjectMD} itself
     */
    setAmzServerSideEncryption(serverSideEncryption) {
        this.data['x-amz-server-side-encryption'] = serverSideEncryption;
        return this;
    }

    /**
     * Returns server side encryption
     *
     * @return {string} server side encryption
     */
    getAmzServerSideEncryption() {
        return this.data['x-amz-server-side-encryption'];
    }

    /**
     * Set encryption key id
     *
     * @param {string} keyId - Encryption key id
     * @return {ObjectMD} itself
     */
    setAmzEncryptionKeyId(keyId) {
        this.data['x-amz-server-side-encryption-aws-kms-key-id'] = keyId;
        return this;
    }

    /**
     * Returns encryption key id
     *
     * @return {string} Encryption key id
     */
    getAmzEncryptionKeyId() {
        return this.data['x-amz-server-side-encryption-aws-kms-key-id'];
    }

    /**
     * Set encryption customer algorithm
     *
     * @param {string} algo - Encryption customer algorithm
     * @return {ObjectMD} itself
     */
    setAmzEncryptionCustomerAlgorithm(algo) {
        this.data['x-amz-server-side-encryption-customer-algorithm'] = algo;
        return this;
    }

    /**
     * Returns Encryption customer algorithm
     *
     * @return {string} Encryption customer algorithm
     */
    getAmzEncryptionCustomerAlgorithm() {
        return this.data['x-amz-server-side-encryption-customer-algorithm'];
    }

    /**
     * Set access control list
     *
     * @param {object} acl - Access control list
     * @param {string} acl.Canned -
     * @param {string[]} acl.FULL_CONTROL -
     * @param {string[]} acl.WRITE_ACP -
     * @param {string[]} acl.READ -
     * @param {string[]} acl.READ_ACP -
     * @return {ObjectMD} itself
     */
    setAcl(acl) {
        this.data.acl = acl;
        return this;
    }

    /**
     * Returns access control list
     *
     * @return {object} Access control list
     */
    getAcl() {
        return this.data.acl;
    }

    /**
     * Set object key
     *
     * @param {string} key - Object key
     * @return {ObjectMD} itself
     */
    setKey(key) {
        this.data.key = key;
        return this;
    }

    /**
     * Returns object key
     *
     * @return {string} object key
     */
    getKey() {
        return this.data.key;
    }

    /**
     * Set location
     *
     * @param {string[]} location - location
     * @return {ObjectMD} itself
     */
    setLocation(location) {
        this.data.location = location;
        return this;
    }

    /**
     * Returns location
     *
     * @return {string[]} location
     */
    getLocation() {
        return this.data.location;
    }

    /**
     * Set custom headers
     *
     * @param {object} data - Headers
     * @return {ObjectMD} itself
     */
    setHeaders(data) {
        Object.keys(data).forEach(key => { this.data[key] = data[key]; });
        return this;
    }

    /**
     * Returns metadata object
     *
     * @return {object} metadata object
     */
    getData() {
        return this.data;
    }
}
