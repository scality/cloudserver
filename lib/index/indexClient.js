import net from 'net';
import index from './utils';

let client;
let callback;

export default {
    connect(port, host) {
        client = new net.Socket();
        client.connect(port, host, function() {
        });
        client.on('data', function(data) {
            data = data.toString();
            data = JSON.parse(data);
            if (data.op === 2) {
                data.params.cb = callback;
                index.constructResponse(data.result, data.params);
            }
        });
        client.on('close', function() {
        });
    },

    putObjectMD(bucketName, objName, objVal) {
        let msg = `1#${bucketName}#${objName}#${objVal['content-length']}#${objVal['content-type']}#${objVal['last-modified']}#${JSON.stringify(objVal.acl)}`;
        Object.keys(objVal).forEach(key => {
            if (key.indexOf('x-amz-meta') !== -1 && key !== 'x-amz-meta-s3cmd-attrs') {
                msg = msg + `#` + key;
                msg = msg + `#` + objVal[key];
            }
        });
        msg = msg + `||`;
        write(msg, null);
    },

    listObject(bucketName, query, prefix, marker, delimiter, maxKeys, cb) {
        let msg = `2#${bucketName}#${prefix}#${marker}#${maxKeys}#${delimiter}`
        for (var i=0; i<query.length; i++) {
            msg = msg + '#' + query[i];
        }
        write(msg, cb);
    },

    deleteObjectMD(bucketName, objName) {
        write(`3#${bucketName}#${objName}||`, null);
    }
}

function write(string, cb) {
    client.write(string);
    callback = cb;
}
