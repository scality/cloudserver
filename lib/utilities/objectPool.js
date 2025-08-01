/**
 * Simple Object Pool to reduce GC pressure by reusing objects
 * Usage: const pool = new ObjectPool(() => ({ key: '', value: null }));
 */
class ObjectPool {
    constructor(createFn, resetFn, maxSize = 100) {
        this.createFn = createFn;
        this.resetFn = resetFn || ((obj) => obj);
        this.pool = [];
        this.maxSize = maxSize;
    }

    acquire() {
        if (this.pool.length > 0) {
            return this.pool.pop();
        }
        return this.createFn();
    }

    release(obj) {
        if (this.pool.length < this.maxSize) {
            this.resetFn(obj);
            this.pool.push(obj);
        }
    }

    size() {
        return this.pool.length;
    }
}

// Pre-configured pools for common objects
const pools = {
    // For frequently created metadata store params
    metadataStoreParams: new ObjectPool(
        () => ({
            objectKey: '',
            authInfo: null,
            metaHeaders: {},
            size: 0,
            contentMD5: '',
            headers: {},
            isDeleteMarker: false,
            replicationInfo: null,
            log: null,
        }),
        (obj) => {
            // Reset object properties
            obj.objectKey = '';
            obj.authInfo = null;
            obj.metaHeaders = {};
            obj.size = 0;
            obj.contentMD5 = '';
            obj.headers = {};
            obj.isDeleteMarker = false;
            obj.replicationInfo = null;
            obj.log = null;
            return obj;
        }
    ),

    // For ACL resource objects
    aclResource: new ObjectPool(
        () => ({
            Canned: '',
            FULL_CONTROL: [],
            WRITE: [],
            WRITE_ACP: [],
            READ: [],
            READ_ACP: [],
        }),
        (obj) => {
            obj.Canned = '';
            obj.FULL_CONTROL.length = 0;
            obj.WRITE.length = 0;
            obj.WRITE_ACP.length = 0;
            obj.READ.length = 0;
            obj.READ_ACP.length = 0;
            return obj;
        }
    ),

    // For version options
    versionOptions: new ObjectPool(
        () => ({
            versionId: '',
            versioning: false,
            isNull: false,
            dataToDelete: null,
            extraMD: {},
        }),
        (obj) => {
            obj.versionId = '';
            obj.versioning = false;
            obj.isNull = false;
            obj.dataToDelete = null;
            obj.extraMD = {};
            return obj;
        }
    ),
};

module.exports = { ObjectPool, pools }; 