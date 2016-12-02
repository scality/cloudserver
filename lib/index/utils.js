import config from '../Config';
import bitmap from 'node-bitmap-ewah';
import { logger } from '../utilities/logger';
import indexd from './bitmapd-utils';
import async from 'async'

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
        }
        else {
            rowId = objMapping.length;
            objMapping.length += 1;
        }
    }
    objMapping.mapping[rowId] = objName;
    objMapping.mapping[objName] = rowId;

    return rowId;
}

function searchIntRange(bucketName, op, term, not, callback) {
    term = term.replace('--integer', '');
    const attr = term.split('/')[0];
    const value = parseInt(term.split('/')[1], 10);
    indexd.get(`${bucketName}/${attr}`, (err, data) => {
        if (err) {
            callback(err, null)
        }
        else {
            const valList = JSON.parse(data)
            if (isNaN(value)) {
                callback(null, bitmap.createObject());
            }
            let nearVal = nearValue(valList, value);
            if (valList[nearVal] < value) {
                nearVal = nearVal + 1;
            }
            indexd.get(`${bucketName}/${attr}/${padLeft(value, 30)}`, (err, term) => {
                if (!err) {
                    term = JSON.parse(term)
                }
                term = storeToBitmap(term)
                indexd.get(`${bucketName}/${attr}/${padLeft(valList[0], 30)}`, (err, lowestValue) => {
                    if (!err) {
                        lowestValue = JSON.parse(lowestValue)
                    }
                    lowestValue = storeToBitmap(lowestValue)
                    indexd.get(`${bucketName}/${attr}/${padLeft(valList[nearVal], 30)}`, (err, nearestValue) => {
                        if (!err) {
                            nearestValue = JSON.parse(nearestValue)
                        }
                        nearestValue = storeToBitmap(nearestValue)
                        indexd.get(`${bucketName}/${attr}/${padLeft(valList[nearVal-1], 30)}`, (err, prevValue) => {
                            if (!err) {
                                prevValue = JSON.parse(prevValue)
                            }
                            prevValue = storeToBitmap(prevValue)
                            indexd.get(`${bucketName}/${attr}/${padLeft(valList[valList.indexOf(value) + 1], 30)}`, (err, nextValue) => {
                                if (!err) {
                                    nextValue = JSON.parse(nextValue)
                                }
                                nextValue = storeToBitmap(nextValue)
                                let result = bitmap.createObject();
                                if (op.indexOf('=') !== -1) {
                                    if (valList.indexOf(value) !== -1) {
                                        result = result.or(term).xor(nextValue);
                                    }
                                }
                                if (op.indexOf('<') !== -1) {
                                    if (value > valList[valList.length - 1]) {
                                        result = result.or(lowestValue);
                                    } else if (value > valList[0]) {
                                        result = result.or((nearestValue).xor(lowestValue));
                                    }
                                }
                                if (op.indexOf('>') !== -1) {
                                    if (value < valList[0]) {
                                        result = result.or(lowestValue);
                                    } else if (value < valList[valList.length - 1]) {
                                        result = result.or((prevValue).and(nearestValue));
                                    }
                                }
                                parseNotOperator(result, not, callback);
                            })
                        })
                    })
                })
            })
        }
    })
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
        indexd.get(`${bucketName}/${searchTerm}`, (err, data) => {
            if (!err) {
                data = JSON.parse(data)
            }
            parseNotOperator(storeToBitmap(data), not, callback);
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
    indexd.get(`${bucketName}/${searchTerm}`, (err, data) => {
        if (!err) {
            data = JSON.parse(data);
        }
        parseNotOperator(storeToBitmap(data), not, callback);
    })
}

function readContentTypeIndex(bucketName, searchTerm, not, callback) {
    const type = `contenttype/${searchTerm.split('/')[1]}`;
    const subtype = `contentsubtype/${searchTerm.split('/')[2]}`;
    indexd.get(`${bucketName}/${type}`, (err, data) => {
        if (!err) {
            data = JSON.parse(data)
        }
        let result = storeToBitmap(data);
        if (searchTerm.split('/').length > 2) {
            indexd.get(`${bucketName}/${subtype}`, (err, data) => {
                if (!err) {
                    data = JSON.parse(data)
                }
                parseNotOperator(storeToBitmap(data), not, callback);
            });
        } else {
            parseNotOperator(storeToBitmap(data), not, callback);
        }
    })
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
    } else if (searchTerm.indexOf('contenttype') !== -1) {
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

function updateIntIndex(bucketName, attribute, value, rowId) {
    indexd.get(`${bucketName}/${attribute}`, (err, data) => {
        if (err) {
            data = []
        }
        else {
            data = JSON.parse(data)
        }
        if (data.indexOf(value) === -1) {
            data.push(value);
        }
        data.sort(function (a, b) { return a - b; });
        indexd.put(`${bucketName}/${attribute}`, JSON.stringify(data), err =>{
            if (err) {
                return ;
            }
        })

        const ind = data.indexOf(value);
        const nextTag = `${attribute}/${padLeft(data[ind + 1], 30)}`
        indexd.get(`${bucketName}/${nextTag}`, (err, data) => {
            if (err) {
            }
            else {
                data = JSON.parse(data)
            }
            indexd.put(`${bucketName}/${attribute}/${padLeft(value, 30)}`, JSON.stringify(updateBitmap(storeToBitmap(data), rowId)), err =>{
                if (err) {
                    return ;
                }
            })
        })

        let end = ind - 1;
        if (end < 0) {
            end = 0;
        }
        const first = `${attribute}/${padLeft(data[0], 30)}`
        const last = `${attribute}/${padLeft(data[end], 30)}`
        indexd.getRange(`${bucketName}/${first}`, `${bucketName}/${last}`, (err, list) => {
            if (list.length > 0) {
                var ops = []
                list.forEach(elem => {
                    elem.value = JSON.parse(elem.value)
                    const index = storeToBitmap(elem.value);
                    index.push(rowId);
                    ops.push({ 'type':'put', 'key': elem.key, 'value':JSON.stringify(bitmapToStore(index))})
                })
                indexd.batchWrite(ops, (err) =>{
                    if (err) {
                        return ;
                    }
                })
            }
        })
    })
}

function updateACLIndex(bucketName, objVal, rowId) {
    deleteOldEntries(bucketName, rowId, () => {
        Object.keys(objVal).forEach(elem => {
            if (typeof objVal[elem] === 'string') {
                updateIndexEntry(`${bucketName}/acl/${elem}/${objVal[elem]}`, rowId)
            } else {
                objVal[elem].forEach(item => {
                    updateIndexEntry(`${bucketName}/acl/${elem}/${item}`, rowId)
                });
            }
        });
    });
}

function updateContentTypeIndex(bucketName, objVal, rowId) {
    updateIndexEntry(`${bucketName}/contenttype/${objVal.split('/')[0]}`, rowId)
    updateIndexEntry(`${bucketName}/contentsubtype/${objVal.split('/')[1]}`, rowId)
}

function updateΜodDateIndex(bucketName, objVal, rowId) {
    const date = new Date(objVal);
    const term = 'modificationdate-';
    const year = date.getUTCFullYear();
    const month = date.getUTCMonth() + 1;
    const day = date.getUTCDate();
    const hours = date.getUTCHours();
    const minutes = date.getUTCMinutes();
    updateIntIndex(bucketName, `${term}year`, year, rowId);
    updateIntIndex(bucketName, `${term}month`, month, rowId);
    updateIntIndex(bucketName, `${term}day`, day, rowId);
    updateIntIndex(bucketName, `${term}hours`, hours, rowId);
    updateIntIndex(bucketName, `${term}minutes`, minutes, rowId);
}

function updateFileSizeIndex(bucketName, objVal, rowId) {
    updateIntIndex(bucketName, `filesize`, parseInt(objVal, 10), rowId);
}

function updateTagIndex(bucketName, objVal, rowId) {
    const tags = [];
    Object.keys(objVal).forEach(elem => {
        if (elem.indexOf('x-amz-meta') !== -1 &&
                elem !== 'x-amz-meta-s3cmd-attrs') {
            tags.push({key: elem, value: objVal[elem]});
        }
    });
    tags.forEach(tag => {
        if (tag.key.indexOf('--integer') !== -1) {
            tag.key = tag.key.replace('--integer', '');
            updateIntIndex(bucketName, tag.key, parseInt(tag.value, 10), rowId);
        } else {
            updateIndexEntry(`${bucketName}/${tag.key}/${tag.value}`, rowId)
        }
    });
}

const index = {
    evaluateQuery: (queryTerms, params) => {
        const bucketName = params.bucketName;
        async.map(queryTerms, readIndex.bind(null, bucketName), function(err, queryTerms) {
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
                updateFileSizeIndex(bucketName, objVal['content-length'], rowId);
                updateΜodDateIndex(bucketName, objVal['last-modified'], rowId);
                updateContentTypeIndex(bucketName, objVal['content-type'], rowId);
                updateACLIndex(bucketName, objVal['acl'], rowId);
                updateTagIndex(bucketName, objVal, rowId);
        })
    },

    deleteObject: (bucketName, objName) => {
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
    },
};

export default index;
