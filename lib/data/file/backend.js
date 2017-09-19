const arsenal = require('arsenal');
const { config } = require('../../Config');

class DataFileInterface {
    constructor() {
        const { host, port } = config.dataClient;

        this.restClient = new arsenal.network.rest.RESTClient(
            { host, port });
    }

    put(stream, size, keyContext, reqUids, callback) {
        // ignore keyContext
        this.restClient.put(stream, size, reqUids, callback);
    }

    get(objectGetInfo, range, reqUids, callback) {
        const key = objectGetInfo.key ? objectGetInfo.key : objectGetInfo;
        this.restClient.get(key, range, reqUids, callback);
    }

    delete(objectGetInfo, reqUids, callback) {
        const key = objectGetInfo.key ? objectGetInfo.key : objectGetInfo;
        this.restClient.delete(key, reqUids, callback);
    }

    getDiskUsage(reqUids, callback) {
        this.restClient.getAction('diskUsage', reqUids, (err, val) => {
            if (err) {
                return callback(err);
            }
            return callback(null, JSON.parse(val));
        });
    }
}

module.exports = DataFileInterface;
