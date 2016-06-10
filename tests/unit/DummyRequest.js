import http from 'http';

export default class DummyRequest extends http.IncomingMessage {
    constructor(obj, msg) {
        super();
        Object.keys(obj).forEach(x => {
            this[x] = obj[x];
        });
        this.push(msg);
        this.push(null);
    }
}
