export default class Bucket {
    constructor(name, ownerId, ownerDisplayName) {
        this.keyMap = {};
        this.acl = {
            Canned: 'private',
            FULL_CONTROL: [],
            WRITE: [],
            WRITE_ACP: [],
            READ: [],
            READ_ACP: [],
        };
        this.policy = {};
        this.name = name;
        this.owner = ownerId;
        this.ownerDisplayName = ownerDisplayName;
        this.creationDate = new Date;
    }
}
