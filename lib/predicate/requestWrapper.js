import stream from 'stream';
import crypto from 'crypto';

const reqSym = Symbol('req');
const bufSym = Symbol('buf');
const outputSym = Symbol('output');

class OutputStream extends stream.Readable {
    constructor(reqWrap, options) {
        super(options);
        this[bufSym] = reqWrap[bufSym];
        const req = reqWrap[reqSym];
        this.query = req.query;
        this.namespace = req.namespace;
        this.gotBucketNameFromHost = req.gotBucketNameFromHost;
        this.bucketName = req.bucketName;
        this.objectKey = req.objectKey;
        this.parsedHost = req.parsedHost;
        this.path = req.path;
        this.headers = req.headers;
        this.contentMD5 = reqWrap.contentMD5;
        this.parsedContentLength = reqWrap.parsedContentLength;
        this.on('end', () => {
            this.contentMD5 = reqWrap.contentMD5;
            this.parsedContentLength = reqWrap.parsedContentLength;
        });
    }

  // eslint-disable-next-line no-unused-vars
    _read(size) {
        for (;;) {
            const chunk = this[bufSym].shift();
            if (!chunk) {
                this.push(null);
                break;
            }
            if (!this.push(chunk)) {
                break;
            }
        }
    }
}

class RequestWrapper extends stream.Duplex {
    constructor(req, options) {
        super(options);
        req.pause();
        this[reqSym] = req;
        this[bufSym] = [];
        this.beenRead = false;
        this.beenWritten = false;
        this.contentLength = 0;
        this.contentMD5 = req.contentMD5;
        this.parsedContentLength = req.parsedContentLength;
        this.hash = crypto.createHash('md5');
        this.on('finish', () => {
            this.contentMD5 = this.hash.digest('hex');
            this.parsedContentLength = this.contentLength;
        });
    }

    setMode(mode) {
        if (mode !== 'transform') {
            return;
        }
        this.beenRead = true;
    }

    _write(chunk, encoding, callback) {
        if (!this.beenRead) {
            return callback(new Error('Write before read not supported'));
        }
        if (!this.beenWritten) {
            this[bufSym] = [];
            this.beenWritten = true;
            this.contentLength = 0;
        }
        this[bufSym].push(chunk);
        this.hash.update(chunk, encoding);
        this.contentLength += chunk.length;
        return callback();
    }

    // eslint-disable-next-line no-unused-vars
    _read(size) {
        if (!this.beenRead) {
            this.beenRead = true;
        }
        let chunk = this[reqSym].read();
        while (chunk) {
            if (!this.beenWritten) {
                this[bufSym].push(chunk);
                this.contentLength += chunk.length;
            }
            if (!this.push(chunk)) {
                break;
            }
            chunk = this[reqSym].read();
        }
        if (this._readableState.flowing) {
            this.push(null);
        }
    }

    [outputSym]() {
        if (!this.beenRead && !this.beenWritten) {
            return this[reqSym];
        }
        return new OutputStream(this);
    }
}

export { outputSym as OUTPUT };
export default RequestWrapper;
