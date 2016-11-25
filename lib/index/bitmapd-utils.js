import net from 'net';
import levelup from 'levelup';
import leveldown from 'leveldown';
import index from './utils';
import child_process from 'child_process';

const server = net.createServer();
let socket = null;
let db = null;
let callback = null;

export default {
    spawn() {
        const child = child_process.spawn('node', ['./lib/index/bitmapd.js']);
        child.stdout.on('data', (data) => {
            console.log(data.toString());
        });
        child.stderr.on('data', (data) => {
            console.log(data.toString());
        });
        child.on('close', (code) => {
            console.log(data.toString());
        });
    },
    listen(port, host) {
        server.listen(port, host);
        server.on('connection', function(sock) {
            sock.on('data', function(data) {
                data = JSON.parse(data);
                if (data.op === 2) {
                    data.params.cb = callback;
                    index.constructResponse(data.result, data.params);
                }
            });
            socket = sock;
        });
    },
    write(string, cb) {
        socket.write(string);
        callback = cb;
    },
    opendb() {
        leveldown.destroy('./indexdb', function (err) {
            if (!err) {
                db = levelup('./indexdb');
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
