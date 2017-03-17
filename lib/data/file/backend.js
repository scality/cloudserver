import config from '../../Config';
import arsenal from 'arsenal';

class DataFileInterface {

    constructor() {
        const { host, port } = config.dataClient;

        this.restClient = new arsenal.network.rest.RESTClient(
            { host, port, log: config.log });
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
}

export default DataFileInterface;
