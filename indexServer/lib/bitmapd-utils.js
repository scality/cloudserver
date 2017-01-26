const fs = require('fs');
const io = require('socket.io-client');
const levelup = require('levelup');
const leveldown = require('leveldown');
const antidoteClient = require('antidote_ts_client');

let config = fs.readFileSync('./config.json', { encoding: 'utf-8' });
config = JSON.parse(config);

let antidote;
const clients = [];
let db = null;

const utils = {
    connectToS3(host, port, attributes) {
        let client = io.connect(`http://${host}:${port}`, {
            reconnection: true,
        });
        client.on('connect', function() {
            for (let i = 0; i < attributes.length; i++) {
                client.emit('subscribe', attributes[i]);
                client.emit('subscribe', 'put');
            }
        });
        client.on('reconnecting', function(number) {
        });
        client.on('error', function(err) {
        });
        client.on('put', function(msg) {
            require('./utils.js').default.updateIndex(msg.bucketName, msg.objName, msg.objVal);
        });
        client.on('query', function(msg) {
            msg.client = client;
            require('./utils.js').default.evaluateQuery(msg);
        });
        client.on('delete', function(msg) {
            require('./utils.js').default.deleteObject(msg.bucketName, msg.objName);
        });
        clients.push(client)
    },

    connectToDB() {
        antidote = antidoteClient.connect(config.antidote.port, config.antidote.host);
        antidote.defaultBucket = 'index';
        leveldown.destroy(config.leveldb_path, function (err) {
            if (!err) {
                db = levelup(config.leveldb_path);
            }
        });
    },

    updateAntidoteSet(key, elem, cb) {
        const keyset = antidote.set('keys');
        antidote.update(
            keyset.add(key)
        ).then( (resp) => {
            const set = antidote.set(key);
            antidote.update(
                set.add(elem)
            ).then( (resp) => {
                return cb();
            });
        });
    },

    readAntidoteSet(key, cb) {
        const set = antidote.set(key);
        set.read().then(objs => {
            return cb(null, objs);
        });
    },

    removeFromAntidoteSet(key, objName, cb) {
        const set = antidote.set(key);
        antidote.update(
            set.remove(objName)
        ).then( (resp) => {
            return cb();
        });
    },

    respondQuery(params, queryTerms) {
        let client = params.client;
        client.emit('query_response', {
            result: queryTerms,
            id: params.id,
            term: params.term
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

exports.default = utils;
exports.config = config;
