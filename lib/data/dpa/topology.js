'use strict'; // eslint-disable-line strict

const topoMD = require('../../../constants').default.topoMD;

const lightMD = topoMD.map(obj => {
    const md = {
        domain: obj.domain,
        replacement: obj.replacement,
        binImgRange: obj.binImgRange,
    };
    return md;
});

// generate recursively topology
function genTopo(topo, prefix, params, index) {
    const number = params[index].number;
    const domain = params[index].domain;
    const weight = params[index].weight;
    const pref = prefix ? `-${prefix}` : '';
    if (index === 0) {
        topo.MD = lightMD;                              // eslint-disable-line
    }
    for (let idx = 0; idx < number; idx++) {
        const obj = {
            id: `${domain}${pref}-${idx + 1}`,
        };
        if (weight) {
            if (Array.isArray(weight) && weight.length === 2) {
                obj.weight = weight[0] +
                    Math.random() * (weight[1] - weight[0]);
            } else if (!isNaN(weight)) {
                obj.weight = weight;
            }
        }
        const key = `${domain}${idx + 1}`;
        topo[key] = obj;                                // eslint-disable-line
        if (index < params.length - 1) {
            genTopo(topo[key], idx + 1, params, index + 1);
        }
    }
}

// update recursively weight of object = sum of its objects' weigth
function updateWeight(obj) {
    if (Object.keys(obj).every(val => obj[val].constructor !== Object)) {
        obj.leaf = true;                                // eslint-disable-line
        if (!obj.weight) {
            obj.weight = 1;                             // eslint-disable-line
        }
    }
    if (!obj.leaf && obj.constructor === Object) {
        obj.weight = 0;                                 // eslint-disable-line
        Object.keys(obj).forEach(val => {
            if (obj[val].constructor === Object) {
                obj.weight +=                           // eslint-disable-line
                    updateWeight(obj[val]);
            }
        });
    }
    return obj.weight || 0;
}

// generate weight distribution
function genWeightDistr(metadata, obj, depth) {
    if (!obj.leaf && obj.constructor === Object) {
        obj.wdistr = [{                                 // eslint-disable-line
            // ids: Object.keys(obj).map(key => obj[key].id).filter(id =>
            //         id !== undefined),
            ids: Object.keys(obj).filter(key =>
                obj[key].constructor === Object),
            pdf: [],
            cdf: [],
        }];
    }
    Object.keys(obj).forEach(val => {
        if (obj[val].constructor === Object) {
            obj.wdistr[0].pdf.push(obj[val].weight || 0);
            if (!obj[val].leaf) {
                genWeightDistr(metadata, obj[val], depth + 1);
            }
        }
    });
    // normalize weight distributions
    // then update with number of bits for each domain
    if (obj.wdistr[0].pdf.length > 0) {
        const sum = obj.wdistr[0].pdf.reduce((a, b) => a + b);
        if (sum > 0) {
            /* maxValue */
            const maxValue = 1 << (metadata[depth].binImgRange[1] -
                                   metadata[depth].binImgRange[0]);
            // console.log(topoMD[depth].size, maxValue);
            /* normalize topo.MD[depth].wdistr[0].pdf */
            obj.wdistr[0].pdf.forEach((val, idx) => {
                obj.wdistr[0].pdf[idx] =                // eslint-disable-line
                    Math.floor(maxValue * val / sum);
            });
            /* compute cdf from pdf */
            obj.wdistr[0].pdf.reduce((a, b, idx) => {
                obj.wdistr[0].cdf[idx] = a + b;         // eslint-disable-line
                return obj.wdistr[0].cdf[idx];
            }, 0);
            // set last element is maxValue
            obj.wdistr[0].cdf[                          // eslint-disable-line
                obj.wdistr[0].cdf.length - 1] = maxValue;
        }
    }
}

function updateMD(obj) {
    if (!obj.MD) {
        obj.MD = lightMD;                               // eslint-disable-line
    }
    updateWeight(obj);
    genWeightDistr(obj.MD, obj, 0);
}

// create a topology for given levels and dimension
function initTopo(_init) {
    const init = _init || topoMD;
    const topo = {};
    genTopo(topo, '', init, 0);
    updateMD(topo);
    return topo;
}

exports.default = {
    init: initTopo,
    update: updateMD,
};
