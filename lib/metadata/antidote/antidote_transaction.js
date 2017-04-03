let startTx = (() => {
    var _ref = _asyncToGenerator(function* (antidote, cb) {
        let tx = yield antidote.startTransaction();
        return cb(tx);
    });

    return function startTx(_x, _x2) {
        return _ref.apply(this, arguments);
    };
})();

let commitTx = (() => {
    var _ref2 = _asyncToGenerator(function* (tx, cb) {
        yield tx.commit();
        return cb();
    });

    return function commitTx(_x3, _x4) {
        return _ref2.apply(this, arguments);
    };
})();

let readMap = (() => {
    var _ref3 = _asyncToGenerator(function* (tx, key, cb) {
        let map = tx.map(key);
        let result = yield map.read();
        result = result.toJsObject();
        return cb(tx, result);
    });

    return function readMap(_x5, _x6, _x7) {
        return _ref3.apply(this, arguments);
    };
})();

let readSet = (() => {
    var _ref4 = _asyncToGenerator(function* (tx, key, cb) {
        let set = tx.set(key);
        let result = yield set.read();
        return cb(tx, result);
    });

    return function readSet(_x8, _x9, _x10) {
        return _ref4.apply(this, arguments);
    };
})();

let updateMapRegister = (() => {
    var _ref5 = _asyncToGenerator(function* (tx, key, value, cb) {
        let map = tx.map(key);
        yield tx.update(map.register(key).set(value));
        return cb(tx);
    });

    return function updateMapRegister(_x11, _x12, _x13, _x14) {
        return _ref5.apply(this, arguments);
    };
})();

let updateSet = (() => {
    var _ref6 = _asyncToGenerator(function* (tx, key, elem, cb) {
        let set = tx.set(key);
        yield tx.update(set.add(elem));
        return cb(tx);
    });

    return function updateSet(_x15, _x16, _x17, _x18) {
        return _ref6.apply(this, arguments);
    };
})();

function _asyncToGenerator(fn) { return function () { var gen = fn.apply(this, arguments); return new Promise(function (resolve, reject) { function step(key, arg) { try { var info = gen[key](arg); var value = info.value; } catch (error) { reject(error); return; } if (info.done) { resolve(value); } else { return Promise.resolve(value).then(function (value) { step("next", value); }, function (err) { step("throw", err); }); } } return step("next"); }); }; }

var antidoteClient = require('antidote_ts_client');
//var antidote = antidoteClient.connect(8187, "localhost")

//antidote.defaultBucket = 'test';

function connect(port, host) {
    return antidoteClient.connect(port, host);
}

function setBucket(antidote, bucket) {
    return antidote.defaultBucket = bucket;
}

exports.connect = connect;
exports.setBucket = setBucket;
exports.startTx = startTx;
exports.commitTx = commitTx;
exports.readMap = readMap;
exports.readSet = readSet;
exports.updateMapRegister = updateMapRegister;
exports.updateSet = updateSet;
