import config from '../../Config';

const backend = {
    put: function putK(request, size, keyContext, reqUids, callback) {
        const value = [];
        const testKinetic = config.kinetic.instance;
        request.on('data', data => {
            value.push(data);
        }).on('end', err => {
            if (err) {
                return callback(err);
            }
            testKinetic.put(value, {}, callback);
            return undefined;
        });
    },

    get: function getK(key, range, reqUids, callback) {
        const testKinetic = config.kinetic.instance;
        return testKinetic.get(Buffer.from(key), range, callback);
    },

    delete: function delK(keyValue, reqUids, callback) {
        const testKinetic = config.kinetic.instance;
        const key = Buffer.from(keyValue);
        return testKinetic.delete(key, callback);
    },
};

export default backend;
