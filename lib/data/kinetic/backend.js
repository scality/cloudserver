import config from '../../Config';

const backend = {
    put: function putK(request, size, keyContext, reqUids, callback, drive) {
        const value = [];
        const kinetic = config.kinetic.instance;
        request.on('data', data => {
            value.push(data);
        }).on('end', err => {
            if (err) {
                return callback(err);
            }
            const index = kinetic.getSocketIndex(
                drive || config.kinetic.hosts[0]);
            return kinetic.put(
                index, Buffer.concat(value), { force: true }, callback);
        });
    },

    get: function getK(key, range, reqUids, callback, drive) {
        const kinetic = config.kinetic.instance;
        const index = kinetic.getSocketIndex(
            drive || config.kinetic.hosts[0]);
        return kinetic.get(index, Buffer.from(key), range, callback);
    },

    delete: function delK(key, reqUids, callback, drive) {
        const kinetic = config.kinetic.instance;
        const index = kinetic.getSocketIndex(
            drive || config.kinetic.hosts[0]);
        return kinetic.delete(
            index, Buffer.from(key), { force: true }, callback);
    },

    healthcheck: (log, callback) => {
        process.nextTick(
            () => callback(null, { statusCode: 200, statusMessage: 'OK' }));
    },
};

export default backend;
