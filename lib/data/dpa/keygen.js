'use strict'; // eslint-disable-line strict

const crypto = require('crypto');

const sid = '59';
const arc = '7';
function createMd5(str, len) {
    return crypto.createHash('md5').update(str).digest().slice(0, len);
}

/**
 * Create UKS key for object
 *  Bits length         Description
 *  24                  Disperse objects into different store nodes
 *  64                  Object identity
 *  36                  Namespace, Owner, Bucket identity
 *  8                   Service identity
 *  8                   Data placement
 *  16                  Erasure codes identity
 *  4                   ARC identity
 * @param{object} params - parameters for namespace, owner, bucket
 * @param{array} dataPlacement - parameters for data placement
 *  array of max 4 numbers of either 0, 1, 2, 3
 * @param{object} ecParams - parameters for erasure codes
 *  .bc_id: backend id of erasure codes
 *  .k: number of data fragments
 *  .m: number of parity fragments
 * @return{string} - uks key
 */
function genObjKey(params, dataPlacement, ecParams) {
    // random part
    const rand = crypto.randomBytes(11).toString('hex');

    // namespace, owner, bucket part
    const hashNamespace = createMd5(params.namespace, 1); // 8 bits
    // 28 bits
    const _hashOwner = createMd5(params.owner, 4).toString('hex').slice(1);
    // replace first 4 bits by 0 to get useful 28-bit sequence
    const hashOwner = new Buffer(`0${_hashOwner}`, 'hex');
    // 12 bits
    const _hashBucket =
        createMd5(params.bucketName, 2).toString('hex').slice(0, 3);
    // replace last 4 bits by 0 to get useful 12-bit sequence
    const hashBucket = new Buffer(`${_hashBucket}0`, 'hex');
    const nob = new Buffer([
        hashNamespace ^ hashOwner[0],
        hashOwner[1],
        hashOwner[2],
        hashOwner[3] ^ hashBucket[0],
        hashBucket[1],
    ]).toString('hex').slice(0, 9);

    // data placement
    let _dp = '';
    dataPlacement.forEach((val, idx) => {
        _dp |= val << (2 * (3 - idx));
    });
    let dp = _dp.toString(16);
    while (dp.length < 2) {
        dp = `0${dp}`;
    }

    // erasure codes
    const _ec = (ecParams.bc_id << 12) |
               (ecParams.k << 6) | ecParams.m;
    let ec = _ec.toString(16);
    while (ec.length < 4) {
        ec = `0${ec}`;
    }

    const key = [rand, nob.slice(0, 8), sid, dp, nob.slice(8), ec.slice(0, 3),
        arc, ec.slice(3)].join('').toUpperCase();

    return key;
}

/**
 * Create UKS key for fragment of given index
 * @param{string} key - UKS key of object
 * @param{number} fragIdx - fragment index
 * @return{string} - uks key of fragment
 */
function genFragKey(key, fragIdx) {
    const fi = (new Buffer([fragIdx])).toString('hex');
    const dispersion = new Buffer(key.slice(0, 6), 'hex');
    const hash = createMd5(fi, 3);     // 3 bytes
    const newDispersion = new Buffer([
        dispersion[0] ^ hash[0],
        dispersion[1] ^ hash[1],
        dispersion[2] ^ hash[2],
    ]);
    return [newDispersion.toString('hex'),
            key.slice(0, 32),
            fi,
            key.slice(34)].join('').toUpperCase();
}

/**
 * Extract EC parameters from UKS key
 * @param{string} key - UKS key of object
 * @return{object} - erasure codes parameters
 *  .bc_id: erasure codes backend id
 *  .k: number of data fragments
 *  .m: number of parity fragments
 */
function getEC(key) {
    const bc = parseInt(`${key.slice(35, 36)}`, 16);
    const ec = parseInt(`${key.slice(36, 38)}${key.slice(39)}`, 16).toString(2);
    const idx = ec.length - 6;
    const k = parseInt(ec.slice(0, idx), 2);
    const m = parseInt(ec.slice(idx), 2);
    return {
        bc_id: bc,  // eslint-disable-line
        k,
        m,
    };
}

/**
 * Create UKS key for all fragments
 * @param{string} key - UKS key of object
 * @return{array} - array of all fragments' key
 */
function genAllFragKeys(key) {
    const ec = getEC(key);
    const n = ec.k + ec.m;
    const keys = new Array(n);
    for (let idx = 0; idx < n; idx++) {
        keys[idx] = genFragKey(key, idx);
    }
    return keys;
}

exports.keygen = {
    obj: genObjKey,
    frag: genFragKey,
    all: genAllFragKeys,
    ec: getEC,
};
