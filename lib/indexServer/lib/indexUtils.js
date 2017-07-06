const fs = require('fs');
const io = require('socket.io-client');
const antidoteClient = require('antidote_ts_client');
const antidoteCli = require('../../antidoteNodeCli/lib/api.js');

let config = fs.readFileSync('./config.json', { encoding: 'utf-8' });
config = JSON.parse(config);

let antidote;
let antidotedb;
const clients = [];

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
            require('./indexOps.js').default.updateIndex(msg.bucketName, msg.objName, msg.objVal);
        });
        client.on('query', function(msg) {
            msg.client = client;
            require('./indexOps.js').default.evaluateQuery(msg);
        });
        client.on('delete', function(msg) {
            require('./indexOps.js').default.deleteObject(msg.bucketName, msg.objName);
        });
        clients.push(client)
    },

    connectToDB() {
        antidotedb = antidoteCli.connect(config.antidote.port, config.antidote.host);
        antidote = antidoteClient.connect(config.antidote.port, config.antidote.host);
        antidoteCli.setBucket(antidotedb, 'index');
        antidote.defaultBucket = 'index';
    },

    updateAntidoteSet(key, elem, tx, cb) {
        console.log("updateAntidoteSet", key, elem);
        antidoteCli.updateSetTx(tx, 'keys', [key], (tx) => {
            antidoteCli.updateSetTx(tx, key, [elem], (tx) => {
                return cb(tx);
            });
        });
    },

    startTx(cb) {
        antidoteCli.startTx(antidotedb, (tx) => {
            return cb(tx)
        });
    },

    commitTx(cb) {
        antidoteCli.commitTx(tx, () => {
            return cb();
        });
    },

    writeIndex(key, objName, tx, cb) {
        console.log("writeIndex", key, objName);
        antidoteCli.updateSetTx(tx, 'keys', [key], (tx) => {
            antidoteCli.updateSetTx(tx, key, [objName], (tx) => {
                return cb(tx);
            });
        });
    },

    readIndex(key, cb) {
        antidoteCli.readSetTx(tx, key, (tx, data) => {
            return cb(tx, data);
        });
    },

    respondQuery(params, queryTerms) {
        let client = params.client;
        client.emit('query_response', {
            result: queryTerms,
            id: params.id,
            term: params.term
        })
    }
}

exports.default = utils;
exports.config = config;
