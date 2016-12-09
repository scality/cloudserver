import http from 'http'
import io from 'socket.io'
import metadata from '../metadata/wrapper.js'

let iosock;
let callback;

export default {
    createPublisher(port, log) {
        const server = http.createServer(function(req, res){
        });
        server.listen(port);
        iosock = io.listen(server);
        iosock.sockets.on('connection', function(socket) {
            log.info("indexing server connected")
            socket.on('disconnect', function() {
            });
            socket.on('subscribe', function(room) {
                socket.join(room);
            });
            socket.on('unsubscribe', function(room) {
                socket.leave(room);
            });
            socket.on('query_response', function(msg) {
                msg.params.cb = callback;
                metadata.respondQueryGetMD(msg.result, msg.params);
            });
        });
    },

    putObjectMD(bucketName, objName, objVal) {
        iosock.sockets.to('puts').emit('put', {
            bucketName,
            objName,
            objVal
        });
    },

    listObject(bucketName, query, prefix, marker, delimiter, maxKeys, cb) {
        callback = cb;
        iosock.sockets.to('queries').emit('query', {
            query,
            params : {
                bucketName,
                prefix,
                marker,
                maxKeys,
                delimiter,
            }
        });
    },

    deleteObjectMD(bucketName, objName) {
        iosock.sockets.to('deletes').emit('delete', {
            bucketName,
            objName
        });
    },

    processQueryHeader(header) {
        if (!header) {
            return header;
        }
        const queryTerms = header.split('&');
        const query = [];
        for (let i = 0; i < queryTerms.length; i++) {
            if (queryTerms[i].indexOf('op/NOT') === -1) {
                query.push(queryTerms[i]);
            } else {
                query.push(`${queryTerms[i]}&${queryTerms[i + 1]}`);
                i += 1;
            }
        }
        return query;
    }
}
