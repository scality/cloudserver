import { errors } from 'arsenal';
import crypto from 'crypto';
import stream from 'stream';

export const ds = [];
let count = 1; // keys are assessed with if (!key)

export function resetCount() {
    count = 1;
}

export const backend = {
    put: function putMem(request, keyContext, reqUids, callback) {
        let value = new Buffer(0);
        request.on('data', data => value = Buffer.concat([value, data]))
        .on('end', () => {
            request.calculatedHash = crypto.createHash('md5')
                                           .update(value).digest('hex');
            ds[count] = { value, keyContext };
            callback(null, count++);
        });
    },

    get: function getMem(key, reqUids, callback) {
        process.nextTick(() => {
            if (!ds[key]) { return callback(errors.NoSuchKey); }
            const storedBuffer = ds[key].value;
            // To be used for start of get range
            let start = 0;
            // To be used for end of get range
            const end = storedBuffer.length;
            const chunkSize = 64 * 1024; // 64KB
            const val = new stream.Readable({
                read: function read() {
                    // sets this._read under the hood
                    // push data onto the read queue, passing null
                    // will signal the end of the stream (EOF)
                    while (start < end) {
                        const finish = Math.min(start + chunkSize, end);
                        this.push(storedBuffer.slice(start, finish));
                        start += chunkSize;
                    }
                    if (start >= end) {
                        this.push(null);
                    }
                },
            });
            return callback(null, val);
        });
    },

    delete: function delMem(key, reqUids, callback) {
        process.nextTick(() => {
            delete ds[key];
            return callback(null);
        });
    },
};

export default backend;
