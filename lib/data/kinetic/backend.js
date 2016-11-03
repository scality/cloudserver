import config from '../../Config';

const backend = {
    put: function putK(request, size, keyContext, reqUids, callback) {
        const value = [];
        const kinetic = config.kinetic.instance;
        request.on('data', data => {
            value.push(data);
        }).on('end', err => {
            if (err) {
                return callback(err);
            }
            return kinetic.getIt(index => {
                kinetic.put(index, Buffer.concat(value), { force: true }, callback);
            });
        });
    },

    get: function getK(key, range, reqUids, callback) {
        const kinetic = config.kinetic.instance;
        return kinetic.getIt(index => {
            return kinetic.get(index, Buffer.from(key), range, callback);
        });
    },

    delete: function delK(key, reqUids, callback) {
        const kinetic = config.kinetic.instance;
        return kinetic.getIt(index => {
            return kinetic.delete(index, Buffer.from(key), { force: true }, callback);
        })
    },
};

export default backend;
