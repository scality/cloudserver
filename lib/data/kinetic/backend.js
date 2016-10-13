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
            const options = {
                synchronization: 'WRITEBACK', // FLUSH
                connectionID: testKinetic.getConnectionId(),
            };
            testKinetic.put(value, options, callback);
            return undefined;
        });
    },

    get: function getK(key, range, reqUids, callback) {
        const testKinetic = config.kinetic.instance;
        return testKinetic.get(new Buffer(key), range, reqUids, callback);
    },

    delete: function delK(keyValue, reqUids, callback) {
        const testKinetic = config.kinetic.instance;
        const key = Buffer.from(keyValue);
        return testKinetic.delete(key, callback);
    },
};

export default backend;
