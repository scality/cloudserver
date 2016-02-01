import crypto from 'crypto';

const ds = [];
let count = 0;

const backend = {
    put: function putMem(request, keyContext, reqUids, callback) {
        let value = new Buffer(0);
        request.on('data', data => value = Buffer.concat([ value, data ]))
        .on('end', () => {
            request.calculatedMD5 = crypto.createHash('md5')
                .update(value).digest('hex');
            ds[count] = { value, keyContext };
            callback(null, [ count++ ]);
        });
    },

    get: function getMem(keys, reqUids, callback) {
        callback(null, keys.map(key => ds[key].value));
    },

    delete: function delMem(keys, reqUids, callback) {
        keys.forEach(key => { delete ds[key]; });
        callback(null);
    }
};

export default backend;
