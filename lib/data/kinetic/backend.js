import config from '../../Config';

const backend = {
    put: function putK(request, size, keyContext, reqUids, callback) {
        // console.log(util.inspect(config, {showHidden: false, depth: null}));
        const value = [];
        const testKinetic = config.kinetic.instance;
        request.on('data', data => {
            value.push(data);
        }).on('end', err => {
            if (err) {
                return callback(err);
            }
            testKinetic.put(0, value, {}, callback);
            return undefined;
        });
    },

    get: function getK(key, range, reqUids, callback) {
        const testKinetic = config.kinetic.instance;
        return testKinetic.get(0, Buffer.from(key), range, callback);
    },

    delete: function delK(keyValue, reqUids, callback) {
        const testKinetic = config.kinetic.instance;
        const key = Buffer.from(keyValue);
        return testKinetic.delete(0, key, {}, callback);
    },
};

export default backend;
