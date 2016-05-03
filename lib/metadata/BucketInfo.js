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
        this.acl = aclInstance;
        this.name = name;
        this.owner = owner;
        this.ownerDisplayName = ownerDisplayName;
        this.creationDate = creationDate;

        return this;
    }
    /**
    * Serialize the object
    * @return {string} - stringified object
    */
    serialize() {
        return JSON.stringify(this);
    }
    /**
    * Get the ACLs.
    * @return {object} acl
    */
    getAcl() {
        return this.acl;
    }
    /**
    * Set the canned acl's.
    * @param {string} cannedACL - canned ACL being set
    * @return {BucketInfo} - bucket info instance
    */
    setCannedAcl(cannedACL) {
        this.acl.Canned = cannedACL;
        return this;
    }
    /**
    * Set a specific ACL.
    * @param {string} canonicalID - id for account being given access
    * @param {string} typeOfGrant - type of grant being granted
    * @return {BucketInfo} - bucket info instance
    */
    setSpecificAcl(canonicalID, typeOfGrant) {
        this.acl[typeOfGrant].push(canonicalID);
        return this;
    }
    /**
    * Set all ACLs.
    * @param {object} acl - new set of ACLs
    * @return {BucketInfo} - bucket info instance
    */
    setFullAcl(acl) {
        this.acl = acl;
        return this;
    }
    /**
    * Get bucket name.
    * @return {string} - bucket name
    */
    getName() {
        return this.name;
    }
    /**
    * Set bucket name.
    * @param {string} bucketName - new bucket name
    * @return {BucketInfo} - bucket info instance
    */
    setName(bucketName) {
        this.name = bucketName;
        return this;
    }
    /**
    * Get bucket owner.
    * @return {string} - bucket owner's canonicalID
    */
    getOwner() {
        return this.owner;
    }
    /**
    * Set bucket owner.
    * @param {string} ownerCanonicalID - bucket owner canonicalID
    * @return {BucketInfo} - bucket info instance
    */
    setOwner(ownerCanonicalID) {
        this.owner = ownerCanonicalID;
        return this;
    }
    /**
    * Get bucket owner display name.
    * @return {string} - bucket owner dispaly name
    */
    getOwnerDisplayName() {
        return this.ownerDisplayName;
    }
    /**
    * Set bucket owner display name.
    * @param {string} ownerDisplayName - bucket owner display name
    * @return {BucketInfo} - bucket info instance
    */
    setOwnerDisplayName(ownerDisplayName) {
        this.ownerDisplayName = ownerDisplayName;
        return this;
    }
    /**
    * Get bucket creation date.
    * @return {object} - bucket creation date
    */
    getCreationDate() {
        return this.creationDate;
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
