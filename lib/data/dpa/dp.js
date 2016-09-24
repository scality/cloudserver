'use strict'; // eslint-disable-line strict

const dataPath = require('../../Config').default.filePaths.dataPath;

/**
 * The function generate a random set of number
 * @param{object} wdistr - object containing pdf and cdf of weigth distribution
 *  .pdf: contain numbers whose sum = `sum`
 *  .cdf: contain increasing order of numbers. The last number is `sum`
 * @param{boolean} replacement - flag allowing replacement of output numbers
 *  true: output could contain repeated number
 *  false: output contains distint numbers
 * @param{array} keys - array of input's identities
 * @return{array} array of ids.length random components drawn based on the
 *  weight distribution
 */
function wRandom(wdistr, replacement, keys) {
    const len = keys.length;
    let idx;
    // not-found case
    if (!replacement && wdistr.pdf.length < len) {
        return undefined;
    }
    const arr = new Array(len);
    if (replacement && (len === 1 || Math.min.apply(null, wdistr.max) >= len)) {
        for (idx = 0; idx < len; idx++) {
            const nb = parseInt(keys[idx], 2);
            let i = 0;
            while (wdistr.cdf[i] < nb) {
                i++;
            }
            arr[idx] = wdistr.ids[i];
        }
    } else {
        const count = new Array(len).fill(0);
        const pdf = wdistr.pdf.slice();
        let sum = pdf.reduce((a, b) => a + b);
        const arrLen = pdf.length;
        for (idx = 0; idx < len; idx++) {
            let nb = parseInt(keys[idx], 2) % sum;
            let i = 0;
            let j = 0;
            while (nb === 0 && !pdf[j]) {
                j = (j + 1) % arrLen;
            }
            while (nb > 0) {
                if (pdf[i]) {
                    nb -= pdf[i];
                    if (nb <= 0) {
                        j = i;
                    }
                }
                i = (i + 1) % arrLen;
            }
            // j is drawn
            count[j]++;
            if (!wdistr.max || count[j] === wdistr.max[j]) {
                sum -= pdf[j];
                pdf[j] = undefined;                  // eslint-disable-line
            }
            arr[idx] = wdistr.ids[j];
        }
    }
    return arr;
}

/**
 * The function execute a breadth-first-search algo to choose component of
 *  topology
 * @param{object} meta - metadata storage topology
 * @param{array} ids - array of fragments identities
 * @param{object} obj - (sub) storage topology
 * @param{number} depth - depth of exploitation
 * @param{array} indices - indices of fragments
 * @param{array} res - array of chosen component
 * @return{boolean} success flag
 */
function bFS(meta, ids, obj, depth, indices, res) {
    let noError = true;
    const _ids = indices.map(idx => ids[idx].slice(meta[depth].binImgRange[0],
                                                   meta[depth].binImgRange[1]));
    const arr = wRandom(obj.wdistr[0], meta[depth].replacement, _ids);
    if (!arr) {
        return false;
    }
    // update res & nextObj
    const nextObj = {};
    const len = arr.length;
    for (let idx = 0; idx < len; idx++) {
        res[indices[idx]].push(arr[idx]);
        if (!nextObj[arr[idx]]) {
            nextObj[arr[idx]] = [];
        }
        nextObj[arr[idx]].push(indices[idx]);
    }

    Object.keys(nextObj).forEach(val => {
        if (!obj[val].leaf) {
            const flag = bFS(meta, ids, obj[val], depth + 1, nextObj[val], res);
            if (!flag) {
                noError = false;
            }
        }
    });
    return noError;
}

function getLocations(topo, ids, keys) {
    const res = ids.map(() => [dataPath]);
    const indices = Array.from({ length: ids.length }, (v, k) => k);
    const flag = bFS(topo.MD, ids, topo, 0, indices, res);
    if (!flag) {
        return undefined;
    }
    return res.map((path, idx) => `${path.join('/')}/${keys[idx]}`);
}

exports.dp = {
    wRandom,
    getLocations,
};
