const fs = require('fs');
const io = require('socket.io-client');
const levelup = require('levelup');
const leveldown = require('leveldown');
const antidoteClient = require('antidote_ts_client');

let config = fs.readFileSync('./config.json', { encoding: 'utf-8' });
config = JSON.parse(config);

let antidote;
let client = null;
let db = null;

const utils = {
    connectToS3() {
        client = io.connect(`http://${config.S3.host}:${config.S3.port}`, {
            reconnection: true,
        });
        client.on('connect', function() {
            client.emit('subscribe', 'puts');
            client.emit('subscribe', 'deletes');
            client.emit('subscribe', 'queries');
        });
        client.on('reconnecting', function(number) {
        });
        client.on('error', function(err) {
        });
        client.on('put', function(msg) {
            require('./utils.js').default.updateIndex(msg.bucketName, msg.objName, msg.objVal);
        });
        client.on('query', function(msg) {
            if (!msg.params.prefix)
                msg.params.prefix = undefined;
            if (!msg.params.marker)
                msg.params.marker = undefined;
            if (!msg.params.delimiter)
                msg.params.delimiter = undefined;
            require('./utils.js').default.evaluateQuery(msg.query, msg.params);
        });
        client.on('delete', function(msg) {
            require('./utils.js').default.deleteObject(msg.bucketName, msg.objName);
        });
    },

    connectToDB() {
        antidote = antidoteClient.connect(config.antidote.port, config.antidote.host);
        leveldown.destroy(config.leveldb_path, function (err) {
            if (!err) {
                db = levelup(config.leveldb_path);
            }
        });
    },

    updateAntidoteSet(key, elem, cb) {
        const set = antidote.set(key);
        antidote.update(
            set.add(elem)
        ).then( (resp) => {
            return cb();
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
        client.emit('query_response', {
            result: queryTerms,
            params
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
