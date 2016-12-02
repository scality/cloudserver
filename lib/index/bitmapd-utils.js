import fs from 'fs'
import io from 'socket.io-client';
import levelup from 'levelup'
import leveldown from 'leveldown'
import { logger } from '../utilities/logger';

const data = fs.readFileSync('./config.json', { encoding: 'utf-8' });
const config = JSON.parse(data);

let client = null;
let db = null;
const msgarr = [];

export default {
    connectToS3(host, port) {
        client = io.connect(`http://${host}:${port}`, {
            reconnection: true,
        });
        client.on('connect', function() {
            console.log('connected');
            client.emit('subscribe', 'puts');
            client.emit('subscribe', 'deletes');
            client.emit('subscribe', 'queries');
        });
        client.on('reconnecting', function(number) {
        });
        client.on('error', function(err) {
            console.log('error', err);
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

    respondQuery(params, queryTerms) {
        client.emit('query_response', {
            result: queryTerms,
            params
        })
    },

    opendb() {
        leveldown.destroy(config.db, function (err) {
            if (!err) {
                db = levelup(config.db);
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
