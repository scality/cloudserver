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

/* eslint-disable no-param-reassign */
function initWriteBuffer(reqWrap) {
    reqWrap[bufSym] = [];
    reqWrap.beenWritten = true;
    reqWrap.contentLength = 0;
}
/* eslint-enable no-param-reassign */

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
        this.onRequestData = chunk => {
            if (!this.beenWritten) {
                this[bufSym].push(chunk);
                this.contentLength += chunk.length;
            }
            if (!this.push(chunk)) {
                this[reqSym].pause();
            }
        };
        this.onRequestEnd = () => this.push(null);
        this[reqSym].on('data', this.onRequestData);
        this[reqSym].on('end', this.onRequestEnd);
    }

    setMode(mode) {
        if (mode !== 'transform') {
            return;
        }
        this.beenRead = true;
        initWriteBuffer(this);
    }

    _write(chunk, encoding, callback) {
        if (!this.beenRead) {
            return callback(new Error('Write before read not supported ' +
                'outside transform mode'));
        }
        if (!this.beenWritten) {
            initWriteBuffer(this);
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
        this[reqSym].resume();
    }

    [outputSym]() {
        if (!this.beenRead && !this.beenWritten) {
            this[reqSym].removeListener('data', this.onRequestData);
            this[reqSym].removeListener('end', this.onRequestEnd);
            this[reqSym].resume();
            return this[reqSym];
        }
        return new OutputStream(this);
    }
}

export { outputSym as OUTPUT };
export default RequestWrapper;
