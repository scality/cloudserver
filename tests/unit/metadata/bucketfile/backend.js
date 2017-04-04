const EventEmitter = require('events');
const assert = require('assert');

const extensions = require('arsenal').algorithms.list;

const BucketFileInterface =
    require('../../../../lib/metadata/bucketfile/backend').default;

const KEY_LENGTH = 15;
const KEY_COUNT = 1000;
const MAX_KEYS = 100;

const KEY_TEMPLATE = '0'.repeat(KEY_LENGTH + 1);

function zpad(key) {
    return `${KEY_TEMPLATE}${key}`.slice(-KEY_LENGTH);
}

class Reader extends EventEmitter {
    constructor() {
        super();
        this.done = false;
        this.index = 0;
        process.nextTick(() => this.start());
    }

    start() {
        if (this.done) {
            return null;
        }
        const i = this.index++;
        // extensions should take into account maxKeys
        // and should not filter more than that intended value
        assert(i <= MAX_KEYS, `listed more than maxKeys ${MAX_KEYS}`);
        if (i === KEY_COUNT) {
            return this.emit('end');
        }
        this.emit('data', { key: `${zpad(i)}`,
            value: `{"foo":"${i}","initiator":"${i}"}` });
        return process.nextTick(() => this.start());
    }

    destroy() {
        this.done = true;
    }
}

describe('BucketFileInterface::internalListObject', alldone => {
    const bucketfile = new BucketFileInterface();

    // stub db to inspect the extensions
    const db = {
        createReadStream: () => new Reader(),
    };

    // stub functions and components
    const logger = { info: () => {}, debug: () => {}, error: () => {} };
    bucketfile.loadDBIfExists = (bucket, log, callback) => callback(null, db);
    bucketfile.unRef = () => {};

    Object.keys(extensions).forEach(listingType => {
        it(`listing max ${MAX_KEYS} keys using ${listingType}`, done => {
            const params = { listingType, maxKeys: MAX_KEYS };
            // assertion to check if extensions work with maxKeys is in Reader
            bucketfile.internalListObject('foo', params, logger, done);
        });
    }, alldone);
});
