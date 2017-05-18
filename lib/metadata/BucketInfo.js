const assert = require('assert');
const { WebsiteConfiguration } = require('./WebsiteConfiguration');
const ReplicationConfiguration =
    require('../api/apiUtils/bucket/models/ReplicationConfiguration');

// WHEN UPDATING THIS NUMBER, UPDATE MODELVERSION.MD CHANGELOG
const modelVersion = 5;

class BucketInfo {
    /**
    * Represents all bucket information.
    * @constructor
    * @param {string} name - bucket name
    * @param {string} owner - bucket owner's name
    * @param {string} ownerDisplayName - owner's display name
    * @param {object} creationDate - creation date of bucket
    * @param {number} mdBucketModelVersion - bucket model version
    * @param {object} [acl] - bucket ACLs (no need to copy
    * ACL object since referenced object will not be used outside of
    * BucketInfo instance)
    * @param {boolean} transient - flag indicating whether bucket is transient
    * @param {boolean} deleted - flag indicating whether attempt to delete
    * @param {object} serverSideEncryption - sse information for this bucket
    * @param {number} serverSideEncryption.cryptoScheme -
    * cryptoScheme used
    * @param {string} serverSideEncryption.algorithm -
    * algorithm to use
    * @param {string} serverSideEncryption.masterKeyId -
    * key to get master key
    * @param {boolean} serverSideEncryption.mandatory -
    * true for mandatory encryption
    * bucket has been made
    * @param {object} versioningConfiguration - versioning configuration
    * @param {string} versioningConfiguration.Status - versioning status
    * @param {object} versioningConfiguration.MfaDelete - versioning mfa delete
    * @param {string} locationConstraint - locationConstraint for bucket
    * @param {WebsiteConfiguration} [websiteConfiguration] - website
    * configuration
    * @param {object[]} [cors] - collection of CORS rules to apply
    * @param {string} [cors[].id] - optional ID to identify rule
    * @param {string[]} cors[].allowedMethods - methods allowed for CORS request
    * @param {string[]} cors[].allowedOrigins - origins allowed for CORS request
    * @param {string[]} [cors[].allowedHeaders] - headers allowed in an OPTIONS
    * request via the Access-Control-Request-Headers header
    * @param {number} [cors[].maxAgeSeconds] - seconds browsers should cache
    * OPTIONS response
    * @param {string[]} [cors[].exposeHeaders] - headers expose to applications
    * @param {object} [replicationConfiguration] - replication configuration
    */
    constructor(name, owner, ownerDisplayName, creationDate,
                mdBucketModelVersion, acl, transient, deleted,
                serverSideEncryption, versioningConfiguration,
                locationConstraint, websiteConfiguration, cors,
                replicationConfiguration) {
        assert.strictEqual(typeof name, 'string');
        assert.strictEqual(typeof owner, 'string');
        assert.strictEqual(typeof ownerDisplayName, 'string');
        assert.strictEqual(typeof creationDate, 'string');
        if (mdBucketModelVersion) {
            assert.strictEqual(typeof mdBucketModelVersion, 'number');
        }
        if (acl) {
            assert.strictEqual(typeof acl, 'object');
            assert(Array.isArray(acl.FULL_CONTROL));
            assert(Array.isArray(acl.WRITE));
            assert(Array.isArray(acl.WRITE_ACP));
            assert(Array.isArray(acl.READ));
            assert(Array.isArray(acl.READ_ACP));
        }
        if (serverSideEncryption) {
            assert.strictEqual(typeof serverSideEncryption, 'object');
            const { cryptoScheme, algorithm, masterKeyId, mandatory } =
                serverSideEncryption;
            assert.strictEqual(typeof cryptoScheme, 'number');
            assert.strictEqual(typeof algorithm, 'string');
            assert.strictEqual(typeof masterKeyId, 'string');
            assert.strictEqual(typeof mandatory, 'boolean');
        }
        if (versioningConfiguration) {
            assert.strictEqual(typeof versioningConfiguration, 'object');
            const { Status, MfaDelete } = versioningConfiguration;
            assert(Status === undefined ||
                Status === 'Enabled' ||
                Status === 'Suspended');
            assert(MfaDelete === undefined ||
                MfaDelete === 'Enabled' ||
                MfaDelete === 'Disabled');
        }
        if (locationConstraint) {
            assert.strictEqual(typeof locationConstraint, 'string');
        }
        if (websiteConfiguration) {
            assert(websiteConfiguration instanceof WebsiteConfiguration);
            const { indexDocument, errorDocument, redirectAllRequestsTo,
                routingRules } = websiteConfiguration;
            assert(indexDocument === undefined ||
                typeof indexDocument === 'string');
            assert(errorDocument === undefined ||
                typeof errorDocument === 'string');
            assert(redirectAllRequestsTo === undefined ||
                typeof redirectAllRequestsTo === 'object');
            assert(routingRules === undefined ||
                Array.isArray(routingRules));
        }
        if (cors) {
            assert(Array.isArray(cors));
        }
        if (replicationConfiguration) {
            ReplicationConfiguration.validateConfig(replicationConfiguration);
        }
        const aclInstance = acl || {
            Canned: 'private',
            FULL_CONTROL: [],
            WRITE: [],
            WRITE_ACP: [],
            READ: [],
            READ_ACP: [],
        };

        // IF UPDATING PROPERTIES, INCREMENT MODELVERSION NUMBER ABOVE
        this._acl = aclInstance;
        this._name = name;
        this._owner = owner;
        this._ownerDisplayName = ownerDisplayName;
        this._creationDate = creationDate;
        this._mdBucketModelVersion = mdBucketModelVersion || 0;
        this._transient = transient || false;
        this._deleted = deleted || false;
        this._serverSideEncryption = serverSideEncryption || null;
        this._versioningConfiguration = versioningConfiguration || null;
        this._locationConstraint = locationConstraint || null;
        this._websiteConfiguration = websiteConfiguration || null;
        this._replicationConfiguration = replicationConfiguration || null;
        this._cors = cors || null;
        return this;
    }
    /**
    * Serialize the object
    * @return {string} - stringified object
    */
    serialize() {
        const bucketInfos = {
            acl: this._acl,
            name: this._name,
            owner: this._owner,
            ownerDisplayName: this._ownerDisplayName,
            creationDate: this._creationDate,
            mdBucketModelVersion: this._mdBucketModelVersion,
            transient: this._transient,
            deleted: this._deleted,
            serverSideEncryption: this._serverSideEncryption,
            versioningConfiguration: this._versioningConfiguration,
            locationConstraint: this._locationConstraint,
            websiteConfiguration: undefined,
            cors: this._cors,
            replicationConfiguration: this._replicationConfiguration,
        };
        if (this._websiteConfiguration) {
            bucketInfos.websiteConfiguration =
                this._websiteConfiguration.getConfig();
        }
        return JSON.stringify(bucketInfos);
    }
    /**
     * deSerialize the JSON string
     * @param {string} stringBucket - the stringified bucket
     * @return {object} - parsed string
     */
    static deSerialize(stringBucket) {
        const obj = JSON.parse(stringBucket);
        const websiteConfig = obj.websiteConfiguration ?
            new WebsiteConfiguration(obj.websiteConfiguration) : null;
        return new BucketInfo(obj.name, obj.owner, obj.ownerDisplayName,
            obj.creationDate, obj.mdBucketModelVersion, obj.acl,
            obj.transient, obj.deleted, obj.serverSideEncryption,
            obj.versioningConfiguration, obj.locationConstraint, websiteConfig,
            obj.cors, obj.replicationConfiguration);
    }

    /**
     * Returns the current model version for the data structure
     * @return {number} - the current model version set above in the file
     */
    static currentModelVersion() {
        return modelVersion;
    }

    /**
     * Create a BucketInfo from an object
     *
     * @param {object} data - object containing data
     * @return {BucketInfo} Return an BucketInfo
     */
    static fromObj(data) {
        return new BucketInfo(data._name, data._owner, data._ownerDisplayName,
            data._creationDate, data._mdBucketModelVersion, data._acl,
            data._transient, data._deleted, data._serverSideEncryption,
            data._versioningConfiguration, data._locationConstraint,
            data._websiteConfiguration, data._cors,
            data._replicationConfiguration);
    }

    /**
    * Get the ACLs.
    * @return {object} acl
    */
    getAcl() {
        return this._acl;
    }
    /**
    * Set the canned acl's.
    * @param {string} cannedACL - canned ACL being set
    * @return {BucketInfo} - bucket info instance
    */
    setCannedAcl(cannedACL) {
        this._acl.Canned = cannedACL;
        return this;
    }
    /**
    * Set a specific ACL.
    * @param {string} canonicalID - id for account being given access
    * @param {string} typeOfGrant - type of grant being granted
    * @return {BucketInfo} - bucket info instance
    */
    setSpecificAcl(canonicalID, typeOfGrant) {
        this._acl[typeOfGrant].push(canonicalID);
        return this;
    }
    /**
    * Set all ACLs.
    * @param {object} acl - new set of ACLs
    * @return {BucketInfo} - bucket info instance
    */
    setFullAcl(acl) {
        this._acl = acl;
        return this;
    }
    /**
     * Get the server side encryption information
     * @return {object} serverSideEncryption
     */
    getServerSideEncryption() {
        return this._serverSideEncryption;
    }
    /**
     * Set server side encryption information
     * @param {object} serverSideEncryption - server side encryption information
     * @return {BucketInfo} - bucket info instance
     */
    setServerSideEncryption(serverSideEncryption) {
        this._serverSideEncryption = serverSideEncryption;
        return this;
    }
    /**
     * Get the versioning configuration information
     * @return {object} versioningConfiguration
     */
    getVersioningConfiguration() {
        return this._versioningConfiguration;
    }
    /**
     * Set versioning configuration information
     * @param {object} versioningConfiguration - versioning information
     * @return {BucketInfo} - bucket info instance
     */
    setVersioningConfiguration(versioningConfiguration) {
        this._versioningConfiguration = versioningConfiguration;
        return this;
    }
    /**
     * Check that versioning is 'Enabled' on the given bucket.
     * @return {boolean} - `true` if versioning is 'Enabled', otherwise `false`
     */
    isVersioningEnabled() {
        const versioningConfig = this.getVersioningConfiguration();
        return versioningConfig ? versioningConfig.Status === 'Enabled' : false;
    }
    /**
     * Get the website configuration information
     * @return {object} websiteConfiguration
     */
    getWebsiteConfiguration() {
        return this._websiteConfiguration;
    }
    /**
     * Set website configuration information
     * @param {object} websiteConfiguration - configuration for bucket website
     * @return {BucketInfo} - bucket info instance
     */
    setWebsiteConfiguration(websiteConfiguration) {
        this._websiteConfiguration = websiteConfiguration;
        return this;
    }
    /**
     * Set replication configuration information
     * @param {object} replicationConfiguration - replication information
     * @return {BucketInfo} - bucket info instance
     */
    setReplicationConfiguration(replicationConfiguration) {
        this._replicationConfiguration = replicationConfiguration;
        return this;
    }
    /**
     * Get replication configuration information
     * @return {object|null} replication configuration information or `null` if
     * the bucket does not have a replication configuration
     */
    getReplicationConfiguration() {
        return this._replicationConfiguration;
    }
    /**
     * Get cors resource
     * @return {object[]} cors
     */
    getCors() {
        return this._cors;
    }
    /**
     * Set cors resource
     * @param {object[]} rules - collection of CORS rules
     * @param {string} [rules.id] - optional id to identify rule
     * @param {string[]} rules[].allowedMethods - methods allowed for CORS
     * @param {string[]} rules[].allowedOrigins - origins allowed for CORS
     * @param {string[]} [rules[].allowedHeaders] - headers allowed in an
     * OPTIONS request via the Access-Control-Request-Headers header
     * @param {number} [rules[].maxAgeSeconds] - seconds browsers should cache
     * OPTIONS response
     * @param {string[]} [rules[].exposeHeaders] - headers to expose to external
     * applications
     * @return {BucketInfo} - bucket info instance
     */
    setCors(rules) {
        this._cors = rules;
        return this;
    }
    /**
     * get the serverside encryption algorithm
     * @return {string} - sse algorithm used by this bucket
     */
    getSseAlgorithm() {
        if (!this._serverSideEncryption) {
            return null;
        }
        return this._serverSideEncryption.algorithm;
    }
    /**
     * get the server side encryption master key Id
     * @return {string} -  sse master key Id used by this bucket
     */
    getSseMasterKeyId() {
        if (!this._serverSideEncryption) {
            return null;
        }
        return this._serverSideEncryption.masterKeyId;
    }
    /**
    * Get bucket name.
    * @return {string} - bucket name
    */
    getName() {
        return this._name;
    }
    /**
    * Set bucket name.
    * @param {string} bucketName - new bucket name
    * @return {BucketInfo} - bucket info instance
    */
    setName(bucketName) {
        this._name = bucketName;
        return this;
    }
    /**
    * Get bucket owner.
    * @return {string} - bucket owner's canonicalID
    */
    getOwner() {
        return this._owner;
    }
    /**
    * Set bucket owner.
    * @param {string} ownerCanonicalID - bucket owner canonicalID
    * @return {BucketInfo} - bucket info instance
    */
    setOwner(ownerCanonicalID) {
        this._owner = ownerCanonicalID;
        return this;
    }
    /**
    * Get bucket owner display name.
    * @return {string} - bucket owner dispaly name
    */
    getOwnerDisplayName() {
        return this._ownerDisplayName;
    }
    /**
    * Set bucket owner display name.
    * @param {string} ownerDisplayName - bucket owner display name
    * @return {BucketInfo} - bucket info instance
    */
    setOwnerDisplayName(ownerDisplayName) {
        this._ownerDisplayName = ownerDisplayName;
        return this;
    }
    /**
    * Get bucket creation date.
    * @return {object} - bucket creation date
    */
    getCreationDate() {
        return this._creationDate;
    }
    /**
    * Set location constraint.
    * @param {string} location - bucket location constraint
    * @return {BucketInfo} - bucket info instance
    */
    setLocationConstraint(location) {
        this._locationConstraint = location;
        return this;
    }

    /**
    * Get location constraint.
    * @return {string} - bucket location constraint
    */
    getLocationConstraint() {
        return this._locationConstraint;
    }

    /**
     * Set Bucket model version
     *
     * @param {number} version - Model version
     * @return {BucketInfo} - bucket info instance
     */
    setMdBucketModelVersion(version) {
        this._mdBucketModelVersion = version;
        return this;
    }
    /**
     * Get Bucket model version
     *
     * @return {number} Bucket model version
     */
    getMdBucketModelVersion() {
        return this._mdBucketModelVersion;
    }
    /**
    * Add transient flag.
    * @return {BucketInfo} - bucket info instance
    */
    addTransientFlag() {
        this._transient = true;
        return this;
    }
    /**
    * Remove transient flag.
    * @return {BucketInfo} - bucket info instance
    */
    removeTransientFlag() {
        this._transient = false;
        return this;
    }
    /**
    * Check transient flag.
    * @return {boolean} - depending on whether transient flag in place
    */
    hasTransientFlag() {
        return !!this._transient;
    }
    /**
    * Add deleted flag.
    * @return {BucketInfo} - bucket info instance
    */
    addDeletedFlag() {
        this._deleted = true;
        return this;
    }
    /**
    * Remove deleted flag.
    * @return {BucketInfo} - bucket info instance
    */
    removeDeletedFlag() {
        this._deleted = false;
        return this;
    }
    /**
    * Check deleted flag.
    * @return {boolean} - depending on whether deleted flag in place
    */
    hasDeletedFlag() {
        return !!this._deleted;
    }
    /**
     * Check if the versioning mode is on.
     * @return {boolean} - versioning mode status
     */
    isVersioningOn() {
        return this._versioningConfiguration &&
            this._versioningConfiguration.Status === 'Enabled';
    }
}

module.exports = BucketInfo;
