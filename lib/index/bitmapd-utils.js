const net = require('net');
const levelup = require('levelup');
const leveldown = require('leveldown');
const index = require('./utils');

let socket = null;
let db = null;
let callback = null;
const msgarr = [];

var listener = function() {
    let msg = msgarr.shift();
    if (!msg)
        return;
    msg = msg.split('#');
    if (msg[0] === '1') {
        const objVal = {
            'content-length': msg[3],
            'content-type': msg[4],
            'last-modified': msg[5],
            acl: JSON.parse(msg[6])
        }
        for (i=7; i<msg.length; i+=2) {
            objVal[msg[i]] = msg[i+1];
        }
        require('./utils.js').default.updateIndex(msg[1], msg[2], objVal);
    } else if (msg[0] === '2') {
        console.log('server received query', msg);
        const params = {bucketName: msg[1], prefix: msg[2], marker: msg[3], maxKeys: msg[4], delimiter: msg[5]};
        if (params.prefix === 'undefined')
            params.prefix = undefined;
        if (params.marker === 'undefined')
            params.marker = undefined;
        if (params.delimiter === 'undefined')
            params.delimiter = undefined;
        params.maxKeys = parseInt(params.maxKeys);
        const queryTerms = [];
        for (var i=6; i<msg.length; i++) {
            queryTerms.push(msg[i]);
        }
        require('./utils.js').default.evaluateQuery(queryTerms, params, socket);
    } else if (msg[0] === '3') {
        require('./utils.js').default.deleteObject(msg[1], msg[2]);
    }
}

export default {
    createServer() {
        const server = net.createServer();
        server.listen(7000, "127.0.0.1");
        server.on('connection', function(sock) {
            sock.on('data', function(data) {
                data = data.toString();
                data = data.split('||');
                for (let i=0; i<data.length; i++) {
                    if (data[i] !== '') {
                        msgarr.push(data[i]);
                        process.nextTick(listener);
                    }
                }
            });
            socket = sock;
        });
    },

    opendb() {
        leveldown.destroy('../../indexdb', function (err) {
            if (!err) {
                db = levelup('../../indexdb');
            }
        })
    },
    put(key, value, cb) {
        db.put(key, value, function(err) {
            return cb(err);
        });
    },
    get(key, cb) {
        db.get(key, function(err, data) {
            return cb(err, data);
        });
    },

    getPrefix(prefix, cb) {
        const list = []
        db.createReadStream({
            start: prefix,
            end: prefix + "\xFF"
        })
        .on('data', function(data) {
            list.push(data);
        })
        .on('error', function(err) {
            return cb(err, null)
        })
        .on('close', function() {
            if (list.length === 0)
                return cb(null, null)
            return cb(null, list)
        });
    },

    getRange(start, end, cb) {
        const list = []
        db.createReadStream({
            start: start,
            end: end
        })
        .on('data', function(data) {
            list.push(data);
        })
        .on('error', function(err) {
            return cb(err, null)
        })
        .on('close', function() {
            return cb(null, list)
        });
    },

    batchWrite(ops, cb) {
        db.batch(ops, function(err) {
            return cb(err)
        })
    }
}
