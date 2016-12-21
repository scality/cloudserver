const bitmap = require('node-bitmap-ewah');
const indexd = require('./bitmapd-utils').default;
const config = require('./bitmapd-utils').config;
const async = require('async');
const _ = require('underscore')

function intersection(a, b) {
    if (config.backend === "leveldb") {
        return a.and(b);
    } else if (config.backend === "antidote") {
        return _.intersection(a, b);
    }
}

function union(a, b) {
    if (config.backend === "leveldb") {
        return a.and(b);
    } else if (config.backend === "antidote") {
        return _.union(a, b);
    }
}

function difference(u, a) {
    if (config.backend === "leveldb") {
        return a.not()
    } else if (config.backend === "antidote") {
        return _.difference(u, a);
    }
}

function padLeft(nr, n){
    return Array(n-String(nr).length+1).join('0')+nr;
}

function binaryIndexOf(arr, searchElement, op) {
    let minIndex = 0;
    let maxIndex = arr.length - 1;
    let currentIndex;
    let currentElement;

    while (minIndex <= maxIndex) {
        currentIndex = (minIndex + maxIndex) / 2 | 0;
        currentElement = arr[currentIndex];

        if (currentElement < searchElement) {
            minIndex = currentIndex + 1;
        }
        else if (currentElement > searchElement) {
            maxIndex = currentIndex - 1;
        }
        else {
            return currentIndex;
        }
    }
    if (op === '>') {
        if (arr[currentIndex] < searchElement) {
            currentIndex += 1
        }
    } else {
        if (arr[currentIndex] > searchElement) {
            currentIndex -= 1
        }
    }
    if (currentIndex > arr.length-1 || currentIndex < 0) {
        return -1;
    }
    return currentIndex;
}

function storeToBitmap(stored) {
    const bm = bitmap.createObject();
    if (stored) {
        stored[2] = new Buffer(stored[2], 'binary');
        bm.read(stored);
    }
    return bm;
}

function parseNotOperator(result, not, callback) {
    if (not) {
        callback(null, result.not());
    } else {
        callback(null, result);
    }
}

function updateObjectMapping(objMapping, objName) {
    let rowId;
    if (Object.keys(objMapping)) {
        if (typeof objMapping.mapping[objName] === 'number') {
            rowId = objMapping.mapping[objName];
        } else if (objMapping.nextAvail.length > 0) {
            rowId = objMapping.nextAvail[0];
            objMapping.nextAvail.splice(0, 1);
        } else {
            rowId = objMapping.length;
            objMapping.length += 1;
        }
    }
    objMapping.mapping[rowId] = objName;
    objMapping.mapping[objName] = rowId;

    return rowId;
}

function getObjectMeta(bucketName, cb) {
    if (config.backend === "leveldb") {
        return cb(null);
    } else if (config.backend === "antidote") {
        indexd.readAntidoteSet(`${bucketName}`, (err, allObjects) => {
            return cb(allObjects);
        });
    }
}

function updateIndexMeta(bucketName, objName, cb) {
    if (config.backend === "leveldb") {
        let rowId;
        indexd.get(`${bucketName}`, (err, objMapping) => {
            if (err) {
                objMapping = {nextAvail: [], length:1, mapping:{}}
            }
            else {
                objMapping = JSON.parse(objMapping);
            }
            rowId = updateObjectMapping(objMapping, objName);
            indexd.put(`${bucketName}`, JSON.stringify(objMapping), err => {
                if (err) {
                    return ;
                }
                return cb(err, rowId)
            })
        })
    } else if (config.backend === "antidote") {
        indexd.updateAntidoteSet(`${bucketName}`, objName, () => {
            return cb(null, -1);
        });
    }
}

function filterRemoved(results, params, cb) {
    indexd.readAntidoteSet(`${params.bucketName}/removed`, (err, removed) => {
        results = results.filter(elem => {
            return removed.indexOf(elem) === -1;
        });
        return cb(results);
    });
}

function constructRange(value, callback) {
    indexd.readAntidoteSet(value, (err, result) => {
        callback(null, result);
    });
}

function readDB(key, cb) {
    if (config.backend === "leveldb") {
        indexd.get(key, (err, data) => {
            if (!err) {
                data = JSON.parse(data)
            }
            return (err, data)
            //parseNotOperator(storeToBitmap(data), not, callback);
        });
    } else if (config.backend === "antidote") {
        indexd.readAntidoteSet(key, (err, data) => {
            return cb(err, data);
        });
    }
}

function searchIntRange(bucketName, op, term, not, callback) {
    term = term.replace('--integer', '');
    const attr = term.split('/')[0];
    let value = parseInt(term.split('/')[1], 10);
    if (op === '=') {
        readDB(`${bucketName}/${attr}/${value}`, (err, data) =>{
            callback(err, data);
        });
    } else {
        if (config.backend === "antidote") {
            indexd.readAntidoteSet(`${bucketName}/${attr}`, (err, result) => {
                const range = []
                const index = binaryIndexOf(result, value, 1)
                if (index === -1) {
                    return parseNotOperator([], not, callback);
                }
                if (op.indexOf('>') !== -1) {
                    if (op.indexOf('=') === -1) {
                        value += 1
                    }
                    for (let i = index; i < result.length; i+=1) {
                        range.push(`${bucketName}/${attr}/${result[i]}`)
                    }
                } else if (op.indexOf('<') !== -1) {
                    if (op.indexOf('=') === -1) {
                        value -= 1
                    }
                    for (let i = index; i >= 0; i-=1) {
                        range.push(`${bucketName}/${attr}/${result[i]}`)
                    }
                }
                const objRange = [];
                async.map(range, constructRange, function(err, res) {
                    res.forEach(arr => {
                        arr.forEach(elem => {
                            objRange.push(elem)
                        })
                    })
                    objRange.sort();
                    parseNotOperator(objRange, not, callback);
                });
            });
        }
    }
}

function searchRegExp(bucketName, searchTerm, not, callback) {
    const regexp = new RegExp(searchTerm);
    let result = bitmap.createObject();
    indexd.getPrefix(`${bucketName}/x-amz-meta`, (err, list) => {
        list.forEach(elem => {
            if (elem.key.indexOf('/') !== -1) {
                if (regexp.test(elem.key.substring(11))) {
                    result = result.or(storeToBitmap(JSON.parse(elem.value)));
                }
            }
        });
        parseNotOperator(result, not, callback);
    })
}

function readFromLevelDB(key, not, callback) {

}

function readTagIndex(bucketName, searchTerm, not, callback) {
    let term = null;
    let operator = null;
    if (searchTerm.indexOf('--integer') !== -1) {
        operator = searchTerm.split('/')[1];
        term = searchTerm.replace(operator, '');
        term = term.replace('/', '');
        searchIntRange(bucketName, operator, term, not, callback);
    } else if (searchTerm.indexOf('--regexp') !== -1) {
        searchTerm = searchTerm.replace('--regexp', '');
        searchTerm = searchTerm.substring(11);
        searchRegExp(bucketName, searchTerm, not, callback);
    } else {
        readDB(`${bucketName}/${searchTerm}`, (err, data) =>{
            callback(err, data);
        });
    }
}

function readFileSizeIndex(bucketName, searchTerm, not, callback) {
    const operator = searchTerm.split('/')[1];
    let term = searchTerm.replace(operator, '');
    term = term.replace('/', '');
    searchIntRange(bucketName, operator, term, not, callback);
}

function readModDateIndex(bucketName, searchTerm, not, callback) {
    const operator = searchTerm.split('/')[1];
    let term = searchTerm.replace(operator, '');
    term = term.replace('/', '');
    return searchIntRange(bucketName, operator, term, not, callback);
}

function readACLIndex(bucketName, searchTerm, not, callback) {
    readDB(`${bucketName}/${searchTerm}`, (err, data) =>{
        callback(err, data);
    });
}

function readContentTypeIndex(bucketName, searchTerm, not, callback) {
    readDB(`${bucketName}/${searchTerm}`, (err, data) =>{
        callback(err, data);
    });
}

function readIndex(bucketName, searchTerm, callback) {
    if (searchTerm.indexOf('op/AND') !== -1
        || searchTerm.indexOf('op/OR') !== -1
        || searchTerm.indexOf('op/NOT') !== -1) {
            callback(null, searchTerm);
    }
    let notOperator = false;
    let result;
    if (searchTerm.indexOf('x-amz-meta') !== -1) {
        return readTagIndex(bucketName, searchTerm, notOperator, callback);
    } else if (searchTerm.indexOf('filesize') !== -1) {
        return readFileSizeIndex(bucketName, searchTerm, notOperator, callback);
    } else if (searchTerm.indexOf('modificationdate') !== -1) {
        return readModDateIndex(bucketName, searchTerm, notOperator, callback);
    } else if (searchTerm.indexOf('contenttype') !== -1 || searchTerm.indexOf('contentsubtype') !== -1) {
        return readContentTypeIndex(bucketName, searchTerm, notOperator, callback);
    } else if (searchTerm.indexOf('acl') !== -1) {
        return readACLIndex(bucketName, searchTerm, notOperator, callback);
    }
    return result;
}

function bitmapToStore(bitmap) {
    const toStore = bitmap.write();
    toStore[2] = toStore[2].toString('binary');
    return toStore;
}

function deleteOldEntries(bucketName, rowId, cb) {
    if (config.backend === "leveldb") {
        indexd.getPrefix(`${bucketName}/acl/`, (err, list) => {
            if (!list) {
                return cb();
            }
            const ops = []
            list.forEach(elem => {
                const tmp = storeToBitmap(JSON.parse(elem.value));
                tmp.unset(rowId);
                ops.push({ 'type':'put', 'key': elem.key, 'value':JSON.stringify(bitmapToStore(tmp))})
            });
            indexd.batchWrite(ops, (err) =>{
                if (err) {
                    return null
                } else {
                    return cb();
                }
            });
        });
    } else if (config.backend === "antidote") {
        return cb();
    }
}

function updateBitmap(bitmap, rowId) {
    if (bitmap.length() - 1 <= rowId) {
        bitmap.push(rowId);
    } else {
        bitmap.set(rowId);
    }
    return bitmapToStore(bitmap);
}

function updateIndexEntry(key, rowId) {
    indexd.get(key, (err, data) => {
        if (err) {
        }
        else {
            data = JSON.parse(data)
        }
        indexd.put(key, JSON.stringify(updateBitmap(storeToBitmap(data), rowId)), err =>{
            if (err) {
                return ;
            }
        })
    })
}

function updateIntIndex(bucketName, objName, attribute, value, rowId) {
    if (config.backend === "leveldb") {
        updateIndexEntry(`${bucketName}/${attribute}/${value}`, rowId);
    } else if (config.backend === "antidote") {
        indexd.updateAntidoteSet(`${bucketName}/${attribute}/${value}`, objName, () => {
            indexd.updateAntidoteSet(`${bucketName}/${attribute}`, value, () => {});
        });

    }
}

function updateACLIndex(bucketName, objName, objVal, rowId) {
    deleteOldEntries(bucketName, rowId, () => {
        Object.keys(objVal).forEach(elem => {
            if (typeof objVal[elem] === 'string') {
                if (config.backend === "leveldb") {
                    updateIndexEntry(`${bucketName}/acl/${elem}/${objVal[elem]}`, rowId);
                } else if (config.backend === "antidote") {
                    indexd.updateAntidoteSet(`${bucketName}/acl/${elem}/${objVal[elem]}`, objName, () => {});
                }
            } else {
                objVal[elem].forEach(item => {
                    if (config.backend === "leveldb") {
                        updateIndexEntry(`${bucketName}/acl/${elem}/${item}`, rowId);
                    } else if (config.backend === "antidote") {
                        indexd.updateAntidoteSet(`${bucketName}/acl/${elem}/${item}`, objName, () => {});
                    }
                });
            }
        });
    });
}

function updateContentTypeIndex(bucketName, objName, objVal, rowId) {
    const type = objVal.split('/')[0];
    const subtype = objVal.split('/')[1];
    if (config.backend === "leveldb") {
        updateIndexEntry(`${bucketName}/contenttype/${type}`, rowId);
        updateIndexEntry(`${bucketName}/contentsubtype/${subtype}`, rowId);
    } else if (config.backend === "antidote") {
        indexd.updateAntidoteSet(`${bucketName}/contenttype/${type}`, objName, () => {});
        indexd.updateAntidoteSet(`${bucketName}/contentsubtype/${subtype}`, objName, () => {});
    }
}

function updateΜodDateIndex(bucketName, objName, objVal, rowId) {
    const date = new Date(objVal);
    const term = 'modificationdate-';
    const year = date.getUTCFullYear();
    const month = date.getUTCMonth() + 1;
    const day = date.getUTCDate();
    const hours = date.getUTCHours();
    const minutes = date.getUTCMinutes();
    updateIntIndex(bucketName, objName, `${term}year`, year, rowId);
    updateIntIndex(bucketName, objName, `${term}month`, month, rowId);
    updateIntIndex(bucketName, objName, `${term}day`, day, rowId);
    updateIntIndex(bucketName, objName, `${term}hours`, hours, rowId);
    updateIntIndex(bucketName, objName, `${term}minutes`, minutes, rowId);
}

function updateFileSizeIndex(bucketName, objName, objVal, rowId) {
    updateIntIndex(bucketName, objName, `filesize`, parseInt(objVal, 10), rowId);
}

function updateTagIndex(bucketName, objName, objVal, rowId) {
    const tags = [];
    Object.keys(objVal).forEach(elem => {
        if (elem.indexOf('x-amz-meta') !== -1 &&
                elem !== 'x-amz-meta-s3cmd-attrs') {
            tags.push({key: elem.replace('x-amz-meta-', ''), value: objVal[elem]});
        }
    });
    tags.forEach(tag => {
        if (tag.key.indexOf('--integer') !== -1) {
            tag.key = tag.key.replace('--integer', '');
            updateIntIndex(bucketName, objName, `x-amz-meta/${tag.key}`, parseInt(tag.value, 10), rowId);
        } else {
            if (config.backend === "leveldb") {
                updateIndexEntry(`${bucketName}/x-amz-meta/${tag.key}/${tag.value}`, rowId);
            } else if (config.backend === "antidote") {
                indexd.updateAntidoteSet(`${bucketName}/x-amz-meta/${tag.key}/${tag.value}`, objName, () => {});
            }
        }
    });
}

const index = {
    evaluateQuery: (queryTerms, params) => {
        const bucketName = params.bucketName;
        async.map(queryTerms, readIndex.bind(null, bucketName), function(err, queryTerms) {
            getObjectMeta(bucketName, (allObjects) => {
                let i;
                while (queryTerms.length > 1) {
                    for (i = queryTerms.length - 1; i >= 0; i--) {
                        if (queryTerms[i] === 'op/AND'
                            || queryTerms[i] === 'op/OR'
                            || queryTerms[i] === 'op/NOT') {
                                break;
                            }
                    }
                    if (queryTerms[i] === 'op/NOT') {
                        const op1 = allObjects;
                        const op2 = queryTerms[i + 1];
                        queryTerms.splice(i, 2, difference(op1, op2));
                    } else {
                        const op1 = queryTerms[i + 1];
                        const op2 = queryTerms[i + 2];
                        if (queryTerms[i] === 'op/AND') {
                            queryTerms.splice(i, 3, intersection(op1, op2));
                        } else {
                            queryTerms.splice(i, 3, union(op1, op2));
                        }
                    }
                }
                if (config.backend === "leveldb") {
                    indexd.get(`${bucketName}`, (err, objMapping) => {
                        if (err) {
                            return ;
                        }
                        objMapping = JSON.parse(objMapping);
                        queryTerms = queryTerms[0].toString(':').split(':');
                        queryTerms = queryTerms.map(function (elem) {
                            return objMapping.mapping[elem];
                        });
                        indexd.respondQuery(params, queryTerms)
                    });
                } else if (config.backend === "antidote") {
                    filterRemoved(queryTerms[0], params, (results) => {
                        indexd.respondQuery(params, results);
                    });
                }
            });
        });
    },

    updateIndex: (bucketName, objName, objVal) => {
        updateIndexMeta(bucketName, objName, (err, rowId) => {
            updateFileSizeIndex(bucketName, objName, objVal['content-length'], rowId);
            updateΜodDateIndex(bucketName, objName, objVal['last-modified'], rowId);
            updateContentTypeIndex(bucketName, objName, objVal['content-type'], rowId);
            updateACLIndex(bucketName, objName, objVal['acl'], rowId);
            updateTagIndex(bucketName, objName, objVal, rowId);
        });
    },

    deleteObject: (bucketName, objName) => {
        if (config.backend === "leveldb") {
            indexd.get(`${bucketName}`, (err, objMapping) => {
                if (err) {
                    return ;
                }
                objMapping = JSON.parse(objMapping);
                const rowId = objMapping.mapping[objName];
                delete objMapping.mapping[objName];
                delete objMapping.mapping[rowId];
                objMapping.nextAvail.push(rowId);
                indexd.put(`${bucketName}`, JSON.stringify(objMapping), err => {
                    if (err) {
                        return ;
                    }
                    return ;
                });
            });
        } else if (config.backend === "antidote") {
            indexd.updateAntidoteSet(`${bucketName}/removed`, objName, () => {});
        }
    },
};

exports.default = index;
