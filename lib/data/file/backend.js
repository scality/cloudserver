import { errors } from 'arsenal';
import crypto from 'crypto';
import stream from 'stream';
import fs from 'fs';

const STORAGE_PATH = "/data";
const FOLDER_HASH = 3511; // Prime number
let count = 1; // keys are assessed with if (!key)

export function resetCount() {
    count = 1;
}

/*
* Each object/part becomes a file and the files are stored in a directory hash structure
* under STORAGE_PATH
*/

var hashCode = function(str){
    var hash = 0;
    if (str.length == 0) return hash;
    for (var i = 0; i < str.length; i++) {
        var char = str.charCodeAt(i);
        hash = ((hash<<5)-hash)+char;
        hash = hash & hash; // Convert to 32bit integer
    }
    return (hash<0)?-hash:hash;
};


var getFilePath = function (fileName) {
    var hash = hashCode(fileName);
    var folderHashPath = ((hash % FOLDER_HASH)+1).toString();

    return STORAGE_PATH + "/" + folderHashPath + "/" + fileName;
};

export const backend = {
    put: function putFile(request, keyContext, reqUids, callback) {
        let id = count++;
        let filePath = getFilePath(id);
        let hash = crypto.createHash('md5');

        request.pause();

        fs.open(filePath, 'w', (err, fd) => {
            // TODO manage errors
            request.resume();
            request.on('data', data => {
                request.pause(); // Disable data events as we need to wait for fs.write callback
                hash.update(data);
                fs.write(fd, data, 0, data.length, (err, written, buffer) => {
                    if (written != data.length); {
                        console.log("pas cool", written, data.length);
                    }
                    request.resume(); // Allow data events again
                });
            })
                .on('end', () => {
                    request.calculatedHash = hash.digest('hex');
                    callback(null, id);
                    fs.close(fd);
                });
        });
    },

    get: function getFile(key, reqUids, callback) {
        process.nextTick(() => {
            let id = key;
            let filePath = getFilePath(id);

            var rs = fs.createReadStream(filePath,
                {
                    flags: 'r',
                    encoding: null,
                    fd: null,
                    autoClose: true
                    // TODO: include start and end for range get
                });

            return callback(null, rs);
        });
    },

    delete: function delFile(key, reqUids, callback) {
        process.nextTick(() => {
            let id = key;
            let filePath = getFilePath(id);
            return fs.unlink(filePath, callback);
        });
    },
};

export default backend;
