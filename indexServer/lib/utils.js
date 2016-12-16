const bitmap = require('node-bitmap-ewah');
const indexd = require('./bitmapd-utils').default;
const config = require('./bitmapd-utils').config;
const async = require('async');

function padLeft(nr, n){
    return Array(n-String(nr).length+1).join('0')+nr;
}

function nearValue(array, value) {
    let current = 0;
    let diff = Math.abs(value - current);
    for (let i = 0; i < array.length; i++) {
        let tmpdiff = Math.abs(value - array[i]);
        if (tmpdiff < diff) {
            diff = tmpdiff;
            current = i;
        }
    }
    return current;
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

function filterRemoved(results, params) {
    indexd.readAntidoteSet(`${params.bucketName}/removed`, (err, removed) => {
        results = results.filter(elem => {
            return removed.indexOf(elem) === -1;
        });
        indexd.respondQuery(params, results);
    });
}

function searchIntRange(bucketName, op, term, not, callback) {
    term = term.replace('--integer', '');
    const attr = term.split('/')[0];
    const value = parseInt(term.split('/')[1], 10);
    if (config.backend === "leveldb") {
        readFromLevelDB(`${bucketName}/${attr}/${value}`, not, callback);
    } else if (config.backend === "antidote") {
        readFromAntidote(`${bucketName}/${attr}/${value}`, not, callback);
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
    indexd.get(key, (err, data) => {
        if (!err) {
            data = JSON.parse(data)
        }
        parseNotOperator(storeToBitmap(data), not, callback);
    });
}

function readFromAntidote(key, not, callback) {
    indexd.readAntidoteSet(key, (err, result) => {
        if (err) {
            callback(err, null);
        } else {
            callback(null, result);
        }
    });
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
        if (config.backend === "leveldb") {
            readFromLevelDB(`${bucketName}/${searchTerm}`, not, callback);
        } else if (config.backend === "antidote") {
            readFromAntidote(`${bucketName}/${searchTerm}`, not, callback);
        }
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
    if (config.backend === "leveldb") {
        readFromLevelDB(`${bucketName}/${searchTerm}`, not, callback);
    } else if (config.backend === "antidote") {
        readFromAntidote(`${bucketName}/${searchTerm}`, not, callback);
    }
}

function readContentTypeIndex(bucketName, searchTerm, not, callback) {
    if (config.backend === "leveldb") {
        readFromLevelDB(`${bucketName}/${searchTerm}`, not, callback);
    } else if (config.backend === "antidote") {
        readFromAntidote(`${bucketName}/${searchTerm}`, not, callback);
    }

}

function readIndex(bucketName, searchTerm, callback) {
    if (searchTerm.indexOf('op/AND') !== -1
        || searchTerm.indexOf('op/OR') !== -1) {
        callback(null, searchTerm);
    }
    let notOperator = false;
    if (searchTerm.indexOf('op/NOT') !== -1) {
        searchTerm = searchTerm.split('&')[1];
        notOperator = true;
    }
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
        indexd.updateAntidoteSet(`${bucketName}/${attribute}/${value}`, objName, () => {});
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
            if (config.backend === "leveldb") {
                while (queryTerms.length > 1) {
                    let operatorPos = -1;
                    for (let i = queryTerms.length - 1; i >= 0; i--) {
                        if (queryTerms[i] === 'op/AND'
                        || queryTerms[i] === 'op/OR') {
                            operatorPos = i;
                            break;
                        }
                    }
                    if (queryTerms[operatorPos] === 'op/AND') {
                        const op1 = queryTerms[operatorPos + 1];
                        const op2 = queryTerms[operatorPos + 2];
                        queryTerms.splice(operatorPos, 3, op1.and(op2));
                    } else if (queryTerms[operatorPos] === 'op/OR') {
                        const op1 = queryTerms[operatorPos + 1];
                        const op2 = queryTerms[operatorPos + 2];
                        queryTerms.splice(operatorPos, 3, op1.or(op2));
                    }
                }
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
                filterRemoved(queryTerms[0], params);
            }
        });
    },

    updateIndex: (bucketName, objName, objVal) => {
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
            });
                updateFileSizeIndex(bucketName, objName, objVal['content-length'], rowId);
                updateΜodDateIndex(bucketName, objName, objVal['last-modified'], rowId);
                updateContentTypeIndex(bucketName, objName, objVal['content-type'], rowId);
                updateACLIndex(bucketName, objName, objVal['acl'], rowId);
                updateTagIndex(bucketName, objName, objVal, rowId);
        })
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
