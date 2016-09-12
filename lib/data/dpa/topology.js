'use strict'; // eslint-disable-line strict

const topoMD = require('../../../constants').default.topoMD;

// generate recursively topology
function genTopo(topo, prefix, params, index) {
    const number = params[index].number;
    const field = params[index].field;
    const weight = params[index].weight;
    const pref = prefix ? `-${prefix}` : '';
    for (let idx = 0; idx < number; idx++) {
        const obj = {
            id: `${field}${pref}-${idx + 1}`,
        };
        if (weight) {
            if (Array.isArray(weight) && weight.length === 2) {
                obj.weight = weight[0] +
                    Math.random() * (weight[1] - weight[0]);
            } else if (!isNaN(weight)) {
                obj.weight = weight;
            }
        }
        const key = `${field}${idx + 1}`;
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
function genWeightDistr(obj) {
    if (!obj.leaf && obj.constructor === Object) {
        obj.wdistr = [];                                // eslint-disable-line
    }
    Object.keys(obj).forEach(val => {
        if (obj[val].constructor === Object) {
            obj.wdistr.push(obj[val].weight || 0);
            if (!obj[val].leaf) {
                genWeightDistr(obj[val]);
            }
        }
    });
}

function weightProcessing(obj) {
    updateWeight(obj);
    genWeightDistr(obj);
}

// create a topology for given levels and dimension
const defaultInit = topoMD || [{
    field: 'Rack',
    number: 3,
}, {
    field: 'Server',
    number: 3,
}, {
    field: 'Drive',
    number: 5,
    // number of `[min, max]` -> uniformly random between min and max
    weight: [0.2, 1.5],
}];

function initTopo(_init) {
    const init = _init || defaultInit;
    const topo = {};
    genTopo(topo, '', init, 0);
    weightProcessing(topo);
    return topo;
}

exports.default = {
    init: initTopo,
    update: weightProcessing,
};
