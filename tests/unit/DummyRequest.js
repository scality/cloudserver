import http from 'http';

export default class DummyRequest extends http.IncomingMessage {
    constructor(obj, msg) {
        super();
        Object.keys(obj).forEach(x => {
            this[x] = obj[x];
        });
        const contentLength = this.headers['content-length'];
        if (this.parsedContentLength === undefined) {
            if (contentLength !== undefined) {
                this.parsedContentLength = parseInt(contentLength, 10);
            } else if (msg !== undefined) {
                this.parsedContentLength = msg.length;
            } else {
                this.parsedContentLength = 0;
            }
        }

        if (Array.isArray(msg)) {
            msg.forEach(part => {
                this.push(part);
            });
        } else {
            this.push(msg);
        }
        this.push(null);
    }
}
