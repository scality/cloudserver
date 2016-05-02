import assert from 'assert';

export default class BucketInfo {
    /**
    * Represents all bucket information.
    * @constructor
    * @param {string} name - bucket name
    * @param {string} owner - bucket owner's name
    * @param {string} ownerDisplayName - owner's display name
    * @param {object} creationDate - creation date of bucket
    * @param {object} [acl] - bucket ACLs (no need to copy
    * ACL object since referenced object will not be used outside of
    * BucketInfo instance)
    */
    constructor(name, owner, ownerDisplayName, creationDate, acl) {
        assert.strictEqual(typeof name, 'string');
        assert.strictEqual(typeof owner, 'string');
        assert.strictEqual(typeof ownerDisplayName, 'string');
        assert.strictEqual(typeof creationDate, 'string');
        if (acl) {
            assert.strictEqual(typeof acl, 'object');
            assert(Array.isArray(acl.FULL_CONTROL));
            assert(Array.isArray(acl.WRITE));
            assert(Array.isArray(acl.WRITE_ACP));
            assert(Array.isArray(acl.READ));
            assert(Array.isArray(acl.READ_ACP));
        }

        const aclInstance = acl || {
            Canned: 'private',
            FULL_CONTROL: [],
            WRITE: [],
            WRITE_ACP: [],
            READ: [],
            READ_ACP: [],
        };
        this._acl = aclInstance;
        this._name = name;
        this._owner = owner;
        this._ownerDisplayName = ownerDisplayName;
        this._creationDate = creationDate;

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
        };
        return JSON.stringify(bucketInfos);
    }
    /**
     * deSerialize the JSON string
     * @param {string} stringBucket - the stringified bucket
     * @return {object} - parsed string
     */
    static deSerialize(stringBucket) {
        const obj = JSON.parse(stringBucket);
        return new BucketInfo(obj.name, obj.owner, obj.ownerDisplayName,
                              obj.creationDate, obj.acl);
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
        this.locationConstraint = location;
        return this;
    }
}
