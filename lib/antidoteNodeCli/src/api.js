var antidoteClient = require('antidote_ts_client')

function connect(port, host) {
    return antidoteClient.connect(port, host);
}

function setBucket(antidote, bucket) {
    return antidote.defaultBucket = bucket;
}

async function startTx(antidote, cb) {
    let tx = await antidote.startTransaction()
    return cb(tx)
}

async function commitTx(tx, cb) {
    await tx.commit()
    return cb();
}

async function readSetTx(tx, setKey, cb) {
    let set = tx.set(setKey);
    let result = await set.read();
    return cb(tx, result)
}

async function updateSetTx(tx, setKey, keys, cb) {
    let set = tx.set(setKey);
    let ops = [];
    for (let i = 0; i < keys.length; i++) {
        ops.push(set.add(keys[i]));
    }
    await tx.update(ops);
    return cb(tx);
}

async function removeSetTx(tx, setKey, keys, cb) {
    let set = tx.set(setKey);
    let ops = [];
    for (let i = 0; i < keys.length; i++) {
        ops.push(set.remove(keys[i]));
    }
    await tx.update(ops);
    return cb(tx);
}

async function readMapTx(tx, mapKey, cb) {
    let map = tx.map(mapKey);
    let result = await map.read();
    result = result.toJsObject();
    return cb(tx, result)
}

async function updateMapRegisterTx(tx, mapKey, keys, values, cb) {
    let map = tx.map(mapKey);
    let ops = [];
    for (let i = 0; i < keys.length; i++) {
        ops.push(map.register(keys[i]).set(values[i]))
    }
    await tx.update(ops);
    return cb(tx)
}

async function removeMapRegisterTx(tx, mapKey, keys, cb) {
    let map = tx.map(mapKey);
    let ops = [];
    for (let i = 0; i < keys.length; i++) {
        ops.push(map.remove(map.register(keys[i])))
    }
    await tx.update(ops);
    return cb(tx);
}

exports.connect = connect;
exports.setBucket = setBucket;
exports.startTx = startTx;
exports.commitTx = commitTx;
exports.readMapTx = readMapTx;
exports.readSetTx = readSetTx;
exports.updateMapRegisterTx = updateMapRegisterTx;
exports.removeMapRegisterTx = removeMapRegisterTx;
exports.removeSetTx = removeSetTx;
exports.updateSetTx = updateSetTx;
