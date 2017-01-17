import config from '../../Config';

let nbHost = 0;

if (config.kinetic) {
    nbHost = config.kinetic.hosts.length - 1;
}
let indexHost = 0;

const backend = {
    put: function putK(request, size, keyContext, reqUids, callback, drive) {
        const value = [];
        let valueLen = 0;
        const kinetic = config.kinetic.instance;
        request.on('data', data => {
            value.push(data);
            valueLen += data.length;
        }).on('end', err => {
            if (err) {
                return callback(err);
            }
            const index = kinetic.getSocketIndex(
                drive || config.kinetic.hosts[indexHost]);
            if (indexHost < nbHost) {
                ++indexHost;
            } else {
                indexHost = 0;
            }
            return kinetic.put(index, Buffer.concat(value, valueLen),
                               { force: true, synchronization: 'WRITETHROUGH' },
                               callback);
        });
    },

    get: function getK(key, range, reqUids, callback) {
        const kinetic = config.kinetic.instance;
        const keyBuffer = Buffer.from(key);
        const split = keyBuffer.toString().split(':');
        const obj = { host: split[0], port: split[1] };
        const index = kinetic.getSocketIndex(obj);
        return kinetic.get(index, keyBuffer, range, callback);
    },

    delete: function delK(key, reqUids, callback) {
        const kinetic = config.kinetic.instance;
        const keyBuffer = Buffer.from(key);
        const split = keyBuffer.toString().split(':');
        const obj = { host: split[0], port: split[1] };
        const index = kinetic.getSocketIndex(obj);
        return kinetic.delete(index, keyBuffer,
                              { force: true, synchronization: 'WRITETHROUGH' },
                              callback);
    },

    healthcheck: (log, callback) => {
        process.nextTick(
            () => callback(null, { statusCode: 200, statusMessage: 'OK' }));
    },
};

export default backend;
