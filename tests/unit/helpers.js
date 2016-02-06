import AuthInfo from '../../lib/auth/AuthInfo';

export function makeid(size) {
    let text = '';
    const possible =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
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
        array[currentIndex] = array[randomIndex];
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
    return new AuthInfo({
        canonicalID: accessKey,
        shortid: 'shortid',
        email: `${accessKey}@l.com`,
        accountDisplayName: accessKey,
    });
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
}
