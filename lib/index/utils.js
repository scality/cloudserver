import BucketClientInterface from '../metadata/bucketclient/backend';
import BucketFileInterface from '../metadata/bucketfile/backend';
import inMemory from '../metadata/in_memory/backend';
import config from '../Config';
import bitmap from 'node-bitmap-ewah';

let client;

if (config.backends.metadata === 'mem') {
    client = inMemory;
} else if (config.backends.metadata === 'file') {
    client = new BucketFileInterface();
} else if (config.backends.metadata === 'scality') {
    client = new BucketClientInterface();
}

function nearValues(array, start, end, value, operator) {
    const mid = Math.floor((start + end) / 2);
    if (array[mid] === value) {
        return [mid, mid];
    }
    if (value < array[0]) {
        return [-1, 0];
    }
    if (value > array[array.length - 1]) {
        return [array.length - 1, array.length];
    }
    if (end - start <= 1) {
        return [start, end];
    } else if (array[mid] < value) {
        return nearValues(array, mid, end, value, operator);
    }
    return nearValues(array, start, mid, value, operator);
}

function storeToBitmap(stored) {
    const bm = bitmap.createObject();
    if (stored) {
        stored[2] = new Buffer(stored[2], 'binary');
        bm.read(stored);
    }
    return bm;
}

function getIntRange(op, term, data) {
    term = term.replace('--integer', '');
    const attr = term.split('/')[0];
    const value = parseInt(term.split('/')[1], 10);
    const valList = data[attr];
    if (isNaN(value)) {
        return bitmap.createObject();
    }
    const nearVals = nearValues(valList, 0, valList.length - 1, value, op);
    let result = bitmap.createObject();
    if (op.indexOf('=') !== -1) {
        const nextValue = `${attr}/${valList[valList.indexOf(value) + 1]}`;
        if (! data.hasOwnProperty(term)) {
            result = result.or(bitmap.createObject());
        } else {
            const op1 = storeToBitmap(data[term]);
            const op2 = storeToBitmap(data[nextValue]);
            result = result.or(op1).xor(op2);
        }
    }
    if (op.indexOf('<') !== -1) {
        const lowestValue = `${attr}/${valList[0]}`;
        const nearestValue = `${attr}, /, ${valList[nearVals[1]]}`;
        if (nearVals[0] === -1) {
            result = result.or(bitmap.createObject());
        } else if (nearVals[1] === valList.length) {
            result = result.or(storeToBitmap(data[lowestValue]));
        } else {
            const op1 = storeToBitmap(data[nearestValue]);
            const op2 = storeToBitmap(data[lowestValue]);
            result = result.or(op1).xor(op2);
        }
    }
    if (op.indexOf('>') !== -1) {
        const lowestValue = `${attr}/${valList[0]}`;
        const nearestValue = `${attr}/${valList[nearVals[0]]}`;
        const nextValue = `${attr}/${valList[nearVals[0] + 1]}`;
        if (nearVals[0] === -1) {
            result = result.or(storeToBitmap(data[lowestValue]));
        } else if (nearVals[1] === valList.length
                || term === nextValue) {
            result = result.or(bitmap.createObject());
        } else {
            const op1 = storeToBitmap(data[nearestValue]);
            const op2 = storeToBitmap(data[lowestValue]);
            result = result.or(op1).and(op2);
        }
    }
    return result;
}

function getObjectsfromXAmzMeta(data, searchTerm) {
    let term = null;
    let operator = null;
    if (searchTerm.indexOf('--integer') !== -1) {
        operator = searchTerm.split('/')[1];
        term = searchTerm.replace(operator, '');
        term = term.replace('/', '');
    } else {
        term = searchTerm;
    }
    if (operator) {
        return getIntRange(operator, term, data);
    }
    return storeToBitmap(data[term]);
}

function getObjectsfromFileSize(data, searchTerm) {
    const operator = searchTerm.split('/')[1];
    let term = searchTerm.replace(operator, '');
    term = term.replace('/', '');
    return getIntRange(operator, term, data);
}

function getObjectsfromModDate(data, searchTerm) {
    const op = searchTerm.split('/')[1];
    const year = searchTerm.split('/')[2];
    const month = searchTerm.split('/')[3];
    const day = searchTerm.split('/')[4];
    const hours = searchTerm.split('/')[5];
    const minutes = searchTerm.split('/')[6];
    const term = 'modificationdate-';
    const yearEq = getIntRange('=', `${term}year/${year}`, data);
    const monthEq = getIntRange('=', `${term}month/${month}`, data);
    const dayEq = getIntRange('=', `${term}day/${day}`, data);
    const hoursEq = getIntRange('=', `${term}hours/${hours}`, data);
    const minutesEq = getIntRange('=', `${term}minutes/${minutes}`, data);
    let result;
    if (op === '=') {
        result = yearEq;
        if (month) {
            result = result.and(monthEq);
        }
        if (day) {
            result = result.and(dayEq);
        }
        if (hours) {
            result = result.and(hoursEq);
        }
        if (minutes) {
            result = result.and(minutesEq);
        }
    } else if (op.indexOf('<') !== -1 || op.indexOf('>') !== -1) {
        result = getIntRange(op, `${term}year/${year}`, data);
        if (month) {
            const tmp = getIntRange(op, `${term}month/${month}`, data);
            result = result.or(yearEq.and(tmp));
        }
        if (day) {
            const tmp = getIntRange(op, `${term}hours/${hours}`, data);
            result = result.or(yearEq.and(monthEq.and(tmp)));
        }
        if (hours) {
            const tmp = getIntRange(op, `${term}minutes/${minutes}`, data);
            result = result.or(yearEq.and(monthEq.and(dayEq.and(tmp))));
        }
    }
    return result;
}

function getObjectsfromACL(data, searchTerm) {
    searchTerm = searchTerm.replace('/', '-');
    return storeToBitmap(data[searchTerm]);
}

function getObjectsfromContentType(data, searchTerm) {
    const type = `${searchTerm.split('/')[0]}/${searchTerm.split('/')[1]}`;
    const subtype = `${searchTerm.split('/')[0]}/${searchTerm.split('/')[2]}`;
    let result = storeToBitmap(data[type]);
    if (searchTerm.split('/') > 2) {
        result = result.and(storeToBitmap(data[subtype]));
    }
    return result;
}

function getRowIds(data, searchTerm) {
    if (searchTerm.indexOf('op/AND') !== -1
        || searchTerm.indexOf('op/OR') !== -1) {
        return searchTerm;
    }
    let notOperator = false;
    if (searchTerm.indexOf('op/NOT') !== -1) {
        searchTerm = searchTerm.split('&')[1];
        notOperator = true;
    }
    let result;
    if (searchTerm.indexOf('x-amz-meta') !== -1) {
        result = getObjectsfromXAmzMeta(data.XAmzMeta, searchTerm);
    } else if (searchTerm.indexOf('filesize') !== -1) {
        result = getObjectsfromFileSize(data.fileSize, searchTerm);
    } else if (searchTerm.indexOf('modificationdate') !== -1) {
        result = getObjectsfromModDate(data.modificationDate, searchTerm);
    } else if (searchTerm.indexOf('contenttype') !== -1) {
        result = getObjectsfromContentType(data.contentType, searchTerm);
    } else if (searchTerm.indexOf('acl') !== -1) {
        result = getObjectsfromACL(data.acl, searchTerm);
    }
    if (notOperator) {
        result.push(data.counter + 1);
        result = result.not();
    }
    return result;
}

function bitmapToStore(bitmap) {
    const toStore = bitmap.write();
    toStore[2] = toStore[2].toString('binary');
    return toStore;
}

function deleteOldEntries(rowId, data) {
    Object.keys(data).forEach(key => {
        if (typeof data[key] === 'object' && key !== 'counter') {
            const tmp = storeToBitmap(data[key]);
            tmp.unset(rowId);
            data[key] = bitmapToStore(tmp);
        }
    });
    return data;
}

function updateBitmap(bitmap, rowId) {
    if (bitmap.length() - 1 <= rowId) {
        bitmap.push(rowId);
    } else {
        bitmap.set(rowId);
    }
    return bitmapToStore(bitmap);
}

function updateIntIndex(tag, rowId, indexData) {
    const tagAttr = tag.split('/')[0];
    const tagValue = parseInt(tag.split('/')[1], 10);
    if (!indexData.hasOwnProperty(tagAttr)) {
        indexData[tagAttr] = [];
    }
    if (indexData[tagAttr].indexOf(tagValue) === -1) {
        indexData[tagAttr].push(tagValue);
    }
    indexData[tagAttr].sort(function (a, b) { return a - b; });
    const ind = indexData[tagAttr].indexOf(tagValue);
    const nextTag = `${tagAttr}/${indexData[tagAttr][ind + 1]}`;
    if (indexData.hasOwnProperty(nextTag) && !indexData.hasOwnProperty(tag)) {
        const bm = storeToBitmap(indexData[nextTag]);
        indexData[tag] = updateBitmap(bm, rowId);
    } else {
        indexData[tag] = updateBitmap(storeToBitmap(indexData[tag]), rowId);
    }
    for (let i = ind - 1; i >= 0; i--) {
        const valueTag = `${tagAttr}/${indexData[tagAttr][i]}`;
        const index = storeToBitmap(indexData[valueTag]);
        index.push(rowId);
        indexData[valueTag] = bitmapToStore(index);
    }
    return indexData;
}

function updateACLIndex(data, objVal, rowId) {
    data = deleteOldEntries(rowId, data);
    Object.keys(objVal).forEach(elem => {
        if (typeof objVal[elem] === 'string') {
            const tag = `acl-${elem}/${objVal[elem]}`;
            const btmap = storeToBitmap(data[tag]);
            data[tag] = updateBitmap(btmap, rowId);
        } else {
            objVal[elem].forEach(item => {
                const tag = `acl-${elem}/${item}`;
                data[tag] = updateBitmap(storeToBitmap(data[tag]), rowId);
            });
        }
    });
}

function updateContentTypeIndex(data, objVal, rowId) {
    const typeTag = `contenttype/${objVal.split('/')[0]}`;
    const subtypeTag = `contentsubtype/${objVal.split('/')[1]}`;
    data[typeTag] = updateBitmap(storeToBitmap(data[typeTag]), rowId);
    if (subtypeTag) {
        data[subtypeTag] = updateBitmap(storeToBitmap(data[subtypeTag]), rowId);
    }
}

function updatemodDateIndex(data, objVal, rowId) {
    const date = new Date(objVal);
    const term = 'modificationdate-';
    const year = date.getUTCFullYear();
    const month = date.getUTCMonth() + 1;
    const day = date.getUTCDate();
    const hours = date.getUTCHours();
    const minutes = date.getUTCMinutes();
    updateIntIndex(`${term}year/${year}`, rowId, data);
    updateIntIndex(`${term}month/${month}`, rowId, data);
    updateIntIndex(`${term}day/${day}`, rowId, data);
    updateIntIndex(`${term}hours/${hours}`, rowId, data);
    updateIntIndex(`${term}minutes/${minutes}`, rowId, data);
}

function updateFileSizeIndex(data, objVal, rowId) {
    const tag = `filesize/${objVal}`;
    updateIntIndex(tag, rowId, data);
}

function updateXAmzMetaIndex(data, objVal, rowId) {
    const tags = [];
    Object.keys(objVal).forEach(elem => {
        if (elem.indexOf('x-amz-meta') !== -1 &&
                elem !== 'x-amz-meta-s3cmd-attrs') {
            tags.push(`${elem}/${objVal[elem]}`);
        }
    });
    if (tags.length === 0) {
        return data;
    }
    tags.forEach(tag => {
        if (tag.indexOf('--integer') !== -1) {
            tag = tag.replace('--integer', '');
            updateIntIndex(tag, rowId, data);
        } else {
            data[tag] = updateBitmap(storeToBitmap(data[tag]), rowId);
        }
    });
}

function constructResponse(data, result, params) {
    const { bucketName, prefix, marker, maxKeys, delimiter, log, cb } = params;
    result = result.toString(':').split(':');
    result = result.map(function (elem) {
        return data[elem];
    });
    client.listObject(bucketName, { prefix, marker, maxKeys, delimiter },
        log, (err, data) => {
            if (err) {
                return cb(err);
            }
            data.Contents = data.Contents.filter(elem => {
                return result.indexOf(elem.key) !== -1;
            });
            return cb(err, data);
        });
}

const index = {
    processQueryHeader: header => {
        if (!header) {
            return header;
        }
        const queryTerms = header.split('&');
        const query = [];
        for (let i = 0; i < queryTerms.length; i++) {
            if (queryTerms[i].indexOf('op/NOT') === -1) {
                query.push(queryTerms[i]);
            } else {
                query.push(`${queryTerms[i]}&${queryTerms[i + 1]}`);
                i += 1;
            }
        }
        return query;
    },

    evaluateQuery: (queryTerms, params) => {
        const bucketName = params.bucketName;
        const log = params.log;
        const cb = params.cb;
        client.getObject(bucketName, 'I|bitmapIndex', log, (err, data) => {
            if (err) {
                return cb(err);
            }
            data = JSON.parse(data);
            queryTerms = queryTerms.map(getRowIds.bind(null, data));
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
            constructResponse(data.objNameToRowID, queryTerms[0], params);
        });
    },

    initRowIds: (bucketName, log, cb) => {
        const data = { objNameToRowID: {}, acl: {}, contentType: {},
        modificationDate: {}, fileSize: {}, XAmzMeta: {} };
        client.putObject(bucketName, 'I|bitmapIndex', JSON.stringify(data),
        log, err => {
            if (err) {
                return cb(err);
            }
            return cb(err);
        });
    },

    updateRowIds: (bucketName, objName, objVal, log, cb) => {
        if (objName.indexOf('..|..') !== -1) {
            return cb(null);
        }
        client.getObject(bucketName, 'I|bitmapIndex', log, (err, data) => {
            if (err) {
                return cb(err);
            }
            data = JSON.parse(data);
            let rowId = 0;
            if (Object.keys(data.objNameToRowID)) {
                if (typeof data.objNameToRowID[objName] === 'number') {
                    rowId = data.objNameToRowID[objName];
                } else {
                    rowId = Object.keys(data.objNameToRowID).length / 2 + 1;
                }
            }
            data.objNameToRowID[rowId] = objName;
            data.objNameToRowID[objName] = rowId;
            if (config.systemMetaIndexing) {
                const fileSizeDt = objVal['content-length'];
                const contentType = objVal['content-type'];
                updateFileSizeIndex(data.fileSize, fileSizeDt, rowId);
                updatemodDateIndex(data.modificationDate, objVal.Date, rowId);
                updateContentTypeIndex(data.contentType, contentType, rowId);
                updateACLIndex(data.acl, objVal.acl, rowId);
            }
            if (config.userMetaIndexing) {
                updateXAmzMetaIndex(data.XAmzMeta, objVal, rowId);
            }
            client.putObject(bucketName, 'I|bitmapIndex', JSON.stringify(data),
            log, err => {
                if (err) {
                    return cb(err);
                }
                return cb(err);
            });
        });
    },
};

export default index;
