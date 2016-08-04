import AuthInfo from '../../lib/auth/AuthInfo';
import constants from '../../constants';
import { metadata } from '../../lib/metadata/in_memory/metadata';
import { resetCount, ds } from '../../lib/data/in_memory/backend';

export function makeid(size) {
    let text = '';
    const possible =
    'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    for (let i = 0; i < size; i += 1) {
        text += possible
            .charAt(Math.floor(Math.random() * possible.length));
    }
    return text;
}

export function shuffle(array) {
    let randomIndex;
    let temporaryValue;
    const length = array.length;
    array.forEach((item, currentIndex, array) => {
        randomIndex = Math.floor(Math.random() * length);
        temporaryValue = array[currentIndex];
        // eslint-disable-next-line no-param-reassign
        array[currentIndex] = array[randomIndex];
        // eslint-disable-next-line no-param-reassign
        array[randomIndex] = temporaryValue;
    });
    return array;
}

export function timeDiff(startTime) {
    const timeArray = process.hrtime(startTime);
    // timeArray[0] is whole seconds
    // timeArray[1] is remaining nanoseconds
    const milliseconds = (timeArray[0] * 1000) + (timeArray[1] / 1e6);
    return milliseconds;
}

export function makeAuthInfo(accessKey) {
    const canIdMap = {
        accessKey1: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7'
            + 'cd47ef2be',
        accessKey2: '79a59df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7'
            + 'cd47ef2bf',
        default: `${accessKey}canonicalID`,
    };
    canIdMap[constants.publicId] = constants.publicId;

    return new AuthInfo({
        canonicalID: canIdMap[accessKey] || canIdMap.default,
        shortid: 'shortid',
        email: `${accessKey}@l.com`,
        accountDisplayName: `${accessKey}displayName`,
    });
}

export function createAlteredRequest(alteredItems, objToAlter,
    baseOuterObj, baseInnerObj) {
    const alteredRequest = Object.assign({}, baseOuterObj);
    const alteredNestedObj = Object.assign({}, baseInnerObj);
    Object.keys(alteredItems).forEach(key => {
        alteredNestedObj[key] = alteredItems[key];
    });
    alteredRequest[objToAlter] = alteredNestedObj;
    return alteredRequest;
}

export function cleanup() {
    metadata.buckets = new Map;
    metadata.keyMaps = new Map;
    // Set data store array back to empty array
    ds.length = 0;
    // Set data store key count back to 1
    resetCount();
}

export class DummyRequestLogger {

    constructor() {
        this.ops = [];
        this.counts = {
            trace: 0,
            debug: 0,
            info: 0,
            warn: 0,
            error: 0,
            fatal: 0,
        };
        this.defaultFields = {};
    }

    trace(msg) {
        this.ops.push(['trace', [msg]]);
        this.counts.trace += 1;
    }

    debug(msg) {
        this.ops.push(['debug', [msg]]);
        this.counts.debug += 1;
    }

    info(msg) {
        this.ops.push(['info', [msg]]);
        this.counts.info += 1;
    }

    warn(msg) {
        this.ops.push(['warn', [msg]]);
        this.counts.warn += 1;
    }

    error(msg) {
        this.ops.push(['error', [msg]]);
        this.counts.error += 1;
    }

    fatal(msg) {
        this.ops.push(['fatal', [msg]]);
        this.counts.fatal += 1;
    }

    getSerializedUids() {
        return 'dummy:Serialized:Uids';
    }

    addDefaultFields(fields) {
        Object.assign(this.defaultFields, fields);
    }

    end() {
        return this;
    }
}
