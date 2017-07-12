
// Version 2 changes the format of the data location property
const modelVersion = 2;

/**
 * Class to manage metadata object for regular s3 objects (instead of
 * mpuPart metadata for example)
 */
module.exports = class ObjectMD {

    /**
     * @constructor
     *
     * @param {number} version - Version of the metadata model
     */
    constructor(version) {
        const now = new Date().toJSON();
        this._data = {
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
            'x-amz-website-redirect-location': '',
            'acl': {
                Canned: 'private',
                FULL_CONTROL: [],
                WRITE_ACP: [],
                READ: [],
                READ_ACP: [],
            },
            'key': '',
            'location': [],
            'isNull': '',
            'nullVersionId': '',
            'isDeleteMarker': '',
            'versionId': undefined, // If no versionId, it should be undefined
            'tags': {},
            'replicationInfo': {
                status: '',
                content: [],
                destination: '',
                storageClass: '',
                role: '',
            },
        };
    }

    /**
     * Returns metadata model version
     *
     * @return {number} Metadata model version
     */
    getModelVersion() {
        return this._data['md-model-version'];
    }

    /**
     * Set owner display name
     *
     * @param {string} displayName - Owner display name
     * @return {ObjectMD} itself
     */
    setOwnerDisplayName(displayName) {
        this._data['owner-display-name'] = displayName;
        return this;
    }

    /**
     * Returns owner display name
     *
     * @return {string} Onwer display name
     */
    getOwnerDisplayName() {
        return this._data['owner-display-name'];
    }

    /**
     * Set owner id
     *
     * @param {string} id - Owner id
     * @return {ObjectMD} itself
     */
    setOwnerId(id) {
        this._data['owner-id'] = id;
        return this;
    }

    /**
     * Returns owner id
     *
     * @return {string} owner id
     */
    getOwnerId() {
        return this._data['owner-id'];
    }

    /**
     * Set cache control
     *
     * @param {string} cacheControl - Cache control
     * @return {ObjectMD} itself
     */
    setCacheControl(cacheControl) {
        this._data['cache-control'] = cacheControl;
        return this;
    }

    /**
     * Returns cache control
     *
     * @return {string} Cache control
     */
    getCacheControl() {
        return this._data['cache-control'];
    }

    /**
     * Set content disposition
     *
     * @param {string} contentDisposition - Content disposition
     * @return {ObjectMD} itself
     */
    setContentDisposition(contentDisposition) {
        this._data['content-disposition'] = contentDisposition;
        return this;
    }

    /**
     * Returns content disposition
     *
     * @return {string} Content disposition
     */
    getContentDisposition() {
        return this._data['content-disposition'];
    }

    /**
     * Set content encoding
     *
     * @param {string} contentEncoding - Content encoding
     * @return {ObjectMD} itself
     */
    setContentEncoding(contentEncoding) {
        this._data['content-encoding'] = contentEncoding;
        return this;
    }

    /**
     * Returns content encoding
     *
     * @return {string} Content encoding
     */
    getContentEncoding() {
        return this._data['content-encoding'];
    }

    /**
     * Set expiration date
     *
     * @param {string} expires - Expiration date
     * @return {ObjectMD} itself
     */
    setExpires(expires) {
        this._data.expires = expires;
        return this;
    }

    /**
     * Returns expiration date
     *
     * @return {string} Expiration date
     */
    getExpires() {
        return this._data.expires;
    }

    /**
     * Set content length
     *
     * @param {number} contentLength - Content length
     * @return {ObjectMD} itself
     */
    setContentLength(contentLength) {
        this._data['content-length'] = contentLength;
        return this;
    }

    /**
     * Returns content length
     *
     * @return {number} Content length
     */
    getContentLength() {
        return this._data['content-length'];
    }

    /**
     * Set content type
     *
     * @param {string} contentType - Content type
     * @return {ObjectMD} itself
     */
    setContentType(contentType) {
        this._data['content-type'] = contentType;
        return this;
    }

    /**
     * Returns content type
     *
     * @return {string} Content type
     */
    getContentType() {
        return this._data['content-type'];
    }

    /**
     * Set last modified date
     *
     * @param {string} lastModified - Last modified date
     * @return {ObjectMD} itself
     */
    setLastModified(lastModified) {
        this._data['last-modified'] = lastModified;
        return this;
    }

    /**
     * Returns last modified date
     *
     * @return {string} Last modified date
     */
    getLastModified() {
        return this._data['last-modified'];
    }

    /**
     * Set content md5 hash
     *
     * @param {string} contentMd5 - Content md5 hash
     * @return {ObjectMD} itself
     */
    setContentMd5(contentMd5) {
        this._data['content-md5'] = contentMd5;
        return this;
    }

    /**
     * Returns content md5 hash
     *
     * @return {string} content md5 hash
     */
    getContentMd5() {
        return this._data['content-md5'];
    }

    /**
     * Set version id
     *
     * @param {string} versionId - Version id
     * @return {ObjectMD} itself
     */
    setAmzVersionId(versionId) {
        this._data['x-amz-version-id'] = versionId;
        return this;
    }

    /**
     * Returns version id
     *
     * @return {string} Version id
     */
    getAmzVersionId() {
        return this._data['x-amz-version-id'];
    }

    /**
     * Set server version id
     *
     * @param {string} versionId - server version id
     * @return {ObjectMD} itself
     */
    setAmzServerVersionId(versionId) {
        this._data['x-amz-server-version-id'] = versionId;
        return this;
    }

    /**
     * Returns server version id
     *
     * @return {string} server version id
     */
    getAmzServerVersionId() {
        return this._data['x-amz-server-version-id'];
    }

    /**
     * Set storage class
     *
     * @param {string} storageClass - Storage class
     * @return {ObjectMD} itself
     */
    setAmzStorageClass(storageClass) {
        this._data['x-amz-storage-class'] = storageClass;
        return this;
    }

    /**
     * Returns storage class
     *
     * @return {string} Storage class
     */
    getAmzStorageClass() {
        return this._data['x-amz-storage-class'];
    }

    /**
     * Set server side encryption
     *
     * @param {string} serverSideEncryption - Server side encryption
     * @return {ObjectMD} itself
     */
    setAmzServerSideEncryption(serverSideEncryption) {
        this._data['x-amz-server-side-encryption'] = serverSideEncryption;
        return this;
    }

    /**
     * Returns server side encryption
     *
     * @return {string} server side encryption
     */
    getAmzServerSideEncryption() {
        return this._data['x-amz-server-side-encryption'];
    }

    /**
     * Set encryption key id
     *
     * @param {string} keyId - Encryption key id
     * @return {ObjectMD} itself
     */
    setAmzEncryptionKeyId(keyId) {
        this._data['x-amz-server-side-encryption-aws-kms-key-id'] = keyId;
        return this;
    }

    /**
     * Returns encryption key id
     *
     * @return {string} Encryption key id
     */
    getAmzEncryptionKeyId() {
        return this._data['x-amz-server-side-encryption-aws-kms-key-id'];
    }

    /**
     * Set encryption customer algorithm
     *
     * @param {string} algo - Encryption customer algorithm
     * @return {ObjectMD} itself
     */
    setAmzEncryptionCustomerAlgorithm(algo) {
        this._data['x-amz-server-side-encryption-customer-algorithm'] = algo;
        return this;
    }

    /**
     * Returns Encryption customer algorithm
     *
     * @return {string} Encryption customer algorithm
     */
    getAmzEncryptionCustomerAlgorithm() {
        return this._data['x-amz-server-side-encryption-customer-algorithm'];
    }

    /**
     * Set metadata redirectLocation value
     *
     * @param {string} redirectLocation - The website redirect location
     * @return {ObjectMD} itself
     */
    setRedirectLocation(redirectLocation) {
        this._data['x-amz-website-redirect-location'] = redirectLocation;
        return this;
    }

    /**
     * Get metadata redirectLocation value
     *
     * @return {string} Website redirect location
     */
    getRedirectLocation() {
        return this._data['x-amz-website-redirect-location'];
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
        this._data.acl = acl;
        return this;
    }

    /**
     * Returns access control list
     *
     * @return {object} Access control list
     */
    getAcl() {
        return this._data.acl;
    }

    /**
     * Set object key
     *
     * @param {string} key - Object key
     * @return {ObjectMD} itself
     */
    setKey(key) {
        this._data.key = key;
        return this;
    }

    /**
     * Returns object key
     *
     * @return {string} object key
     */
    getKey() {
        return this._data.key;
    }

    /**
     * Set location
     *
     * @param {string[]} location - location
     * @return {ObjectMD} itself
     */
    setLocation(location) {
        this._data.location = location;
        return this;
    }

    /**
     * Returns location
     *
     * @return {string[]} location
     */
    getLocation() {
        return this._data.location;
    }

    /**
     * Set metadata isNull value
     *
     * @param {boolean} isNull - Whether new version is null or not
     * @return {ObjectMD} itself
     */
    setIsNull(isNull) {
        this._data.isNull = isNull;
        return this;
    }

    /**
     * Get metadata isNull value
     *
     * @return {boolean} Whether new version is null or not
     */
    getIsNull() {
        return this._data.isNull;
    }

    /**
     * Set metadata nullVersionId value
     *
     * @param {string} nullVersionId - The version id of the null version
     * @return {ObjectMD} itself
     */
    setNullVersionId(nullVersionId) {
        this._data.nullVersionId = nullVersionId;
        return this;
    }

    /**
     * Get metadata nullVersionId value
     *
     * @return {string} The version id of the null version
     */
    getNullVersionId() {
        return this._data.nullVersionId;
    }

    /**
     * Set metadata isDeleteMarker value
     *
     * @param {boolean} isDeleteMarker - Whether object is a delete marker
     * @return {ObjectMD} itself
     */
    setIsDeleteMarker(isDeleteMarker) {
        this._data.isDeleteMarker = isDeleteMarker;
        return this;
    }

    /**
     * Get metadata isDeleteMarker value
     *
     * @return {boolean} Whether object is a delete marker
     */
    getIsDeleteMarker() {
        return this._data.isDeleteMarker;
    }

    /**
     * Set metadata versionId value
     *
     * @param {string} versionId - The object versionId
     * @return {ObjectMD} itself
     */
    setVersionId(versionId) {
        this._data.versionId = versionId;
        return this;
    }

    /**
     * Get metadata versionId value
     *
     * @return {string} The object versionId
     */
    getVersionId() {
        return this._data.versionId;
    }

    /**
     * Set tags
     *
     * @param {object} tags - tags object
     * @return {ObjectMD} itself
     */
    setTags(tags) {
        this._data.tags = tags;
        return this;
    }

    /**
     * Returns tags
     *
     * @return {object} tags object
     */
    getTags() {
        return this._data.tags;
    }

    /**
     * Set replication information
     *
     * @param {object} replicationInfo - replication information object
     * @return {ObjectMD} itself
     */
    setReplicationInfo(replicationInfo) {
        const { status, content, destination, storageClass, role } =
            replicationInfo;
        this._data.replicationInfo = {
            status,
            content,
            destination,
            storageClass: storageClass || '',
            role,
        };
        return this;
    }

    /**
     * Get replication information
     *
     * @return {object} replication object
     */
    getReplicationInfo() {
        return this._data.replicationInfo;
    }

    /**
     * Set custom meta headers
     *
     * @param {object} metaHeaders - Meta headers
     * @return {ObjectMD} itself
     */
    setUserMetadata(metaHeaders) {
        Object.keys(metaHeaders).forEach(key => {
            if (key.startsWith('x-amz-meta-')) {
                this._data[key] = metaHeaders[key];
            }
        });
        // If a multipart object and the acl is already parsed, we update it
        if (metaHeaders.acl) {
            this.setAcl(metaHeaders.acl);
        }
        return this;
    }

    /**
     * overrideMetadataValues (used for complete MPU and object copy)
     *
     * @param {object} headers - Headers
     * @return {ObjectMD} itself
     */
    overrideMetadataValues(headers) {
        Object.assign(this._data, headers);
        return this;
    }

    /**
     * Returns metadata object
     *
     * @return {object} metadata object
     */
    getValue() {
        return this._data;
    }
};
