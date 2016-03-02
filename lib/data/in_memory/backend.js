import crypto from 'crypto';
import stream from 'stream';

const ds = [];
let count = 1; // keys are assessed with if (!key)

const backend = {
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
        if (!ds[key]) { return callback(new Error('NoSuchKey')); }
        const val = new stream.Readable;
        val.push(ds[key].value);
        val.push(null);
        callback(null, val);
    },

    delete: function delMem(key, reqUids, callback) {
        delete ds[key];
        callback(null);
    },
};

export default backend;
