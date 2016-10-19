import BucketClientInterface from '../metadata/bucketclient/backend';
import BucketFileInterface from '../metadata/bucketfile/backend';
import BucketInfo from '../metadata/BucketInfo';
import inMemory from '../metadata/in_memory/backend';
import config from '../Config';
import bitmap from 'node-bitmap-ewah';
import { logger } from '../utilities/logger';
import indexd from './bitmapd-utils';


let client;
let implName;

if (config.backends.metadata === 'mem') {
    client = inMemory;
    implName = 'memorybucket';
} else if (config.backends.metadata === 'file') {
    client = new BucketFileInterface();
    implName = 'bucketfile';
} else if (config.backends.metadata === 'scality') {
    client = new BucketClientInterface();
    implName = 'bucketclient';
}

function nearValue(array, value) {
    let current = array[0];
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

function getIntRange(op, term, data) {
    term = term.replace('--integer', '');
    const attr = term.split('/')[0];
    const value = parseInt(term.split('/')[1], 10);
    const valList = data[attr];
    if (isNaN(value)) {
        return bitmap.createObject();
    }
    let result = bitmap.createObject();
    if (op.indexOf('=') !== -1) {
        const nextValue = `${attr}/${valList[valList.indexOf(value) + 1]}`;
        if (valList.indexOf(value) !== -1) {
            const op1 = storeToBitmap(data[term]);
            const op2 = storeToBitmap(data[nextValue]);
            result = result.or(op1).xor(op2);
        } else {
            result = result.or(bitmap.createObject());
        }
    }
    if (op.indexOf('<') !== -1) {
        let nearVal = nearValue(valList, value);
        if (valList[nearVal] < value) {
            nearVal = nearVal + 1;
        }
        const lowestValue = `${attr}/${valList[0]}`;
        let nearestValue = '';
        if (value <= valList[0]) {
            result = result.or(bitmap.createObject());
        } else if (value > valList[valList.length - 1]) {
            result = result.or(storeToBitmap(data[lowestValue]));
        } else {
            nearestValue = `${attr}/${valList[nearVal]}`;
            const op1 = storeToBitmap(data[nearestValue]);
            const op2 = storeToBitmap(data[lowestValue]);
            result = result.or((op1).xor(op2));
        }
    }
    if (op.indexOf('>') !== -1) {
        let nearVal = nearValue(valList, value);
        if (valList[nearVal] <= value) {
            nearVal = nearVal + 1;
        }
        const lowestValue = `${attr}/${valList[0]}`;
        const nearestValue = `${attr}/${valList[nearVal]}`;
        const prevValue = `${attr}/${valList[nearVal-1]}`;
        if (value < valList[0]) {
            result = result.or(storeToBitmap(data[lowestValue]));
        } else if (value >= valList[valList.length - 1]) {
            result = result.or(bitmap.createObject());
        } else {
            const op1 = storeToBitmap(data[prevValue]);
            const op2 = storeToBitmap(data[nearestValue]);
            result = result.or((op1).and(op2));
        }
    }
    return result;
}

function getfromRegExp(data, searchTerm) {
    const regexp = new RegExp(searchTerm);
    let result = bitmap.createObject();
    Object.keys(data).forEach(key => {
        if (key.indexOf('/') !== -1) {
            if (regexp.test(key.substring(11))) {
                result = result.or(storeToBitmap(data[key]));
            }
        }
    });
    return result;
}

function getObjectsfromXAmzMeta(data, searchTerm) {
    let term = null;
    let operator = null;
    if (searchTerm.indexOf('--integer') !== -1) {
        operator = searchTerm.split('/')[1];
        term = searchTerm.replace(operator, '');
        term = term.replace('/', '');
        return getIntRange(operator, term, data);
    } else if (searchTerm.indexOf('--regexp') !== -1) {
        searchTerm = searchTerm.replace('--regexp', '');
        searchTerm = searchTerm.substring(11);
        return getfromRegExp(data, searchTerm);
    } else {
        return storeToBitmap(data[searchTerm]);
    }
}

function getObjectsfromFileSize(data, searchTerm) {
    const operator = searchTerm.split('/')[1];
    let term = searchTerm.replace(operator, '');
    term = term.replace('/', '');
    return getIntRange(operator, term, data);
}

function getObjectsfromModDate(data, searchTerm) {
    const operator = searchTerm.split('/')[1];
    let term = searchTerm.replace(operator, '');
    term = term.replace('/', '');
    return getIntRange(operator, term, data);
    return result;
}

function getObjectsfromACL(data, searchTerm) {
    searchTerm = searchTerm.replace('/', '-');
    return storeToBitmap(data[searchTerm]);
}

function getObjectsfromContentType(data, searchTerm) {
    const type = `contenttype/${searchTerm.split('/')[1]}`;
    const subtype = `contentsubtype/${searchTerm.split('/')[2]}`;
    let result = storeToBitmap(data[type]);
    if (searchTerm.split('/').length > 2) {
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
    const size = [];
    while (objVal > 1024) {
        size.push(objVal % 1024);
        objVal = Math.floor(objVal/1024);
    }
    size.push(objVal);
    const tag = `filesize-B/${size[0]}`;
    updateIntIndex(tag, rowId, data);
    size.splice(0,1);
    if (size.length > 0) {
        const tag = `filesize-K/${size[0]}`;
        updateIntIndex(tag, rowId, data);
        size.splice(0,1);
    }
    if (size.length > 0) {
        const tag = `filesize-M/${size[0]}`;
        updateIntIndex(tag, rowId, data);
        size.splice(0,1);
    }
    if (size.length > 0) {
        const tag = `filesize-G/${size[0]}`;
        updateIntIndex(tag, rowId, data);
        size.splice(0,1)
    }
}

function updateXAmzMetaIndex(data, objVal, rowId) {
    const tags = [];
    Object.keys(objVal).forEach(elem => {
        if (elem.indexOf('x-amz-meta') !== -1 &&
                elem !== 'x-amz-meta-s3cmd-attrs') {
            tags.push(`${elem}/${objVal[elem]}`);
        }
    });
    tags.forEach(tag => {
        if (tag.indexOf('--integer') !== -1) {
            tag = tag.replace('--integer', '');
            updateIntIndex(tag, rowId, data);
        } else if (tag.indexOf('--integer') !== -1) {
            tag = tag.replace('--integer', '');
            updateIntIndex(tag, rowId, data);
        } else {
            data[tag] = updateBitmap(storeToBitmap(data[tag]), rowId);
        }
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

    evaluateQuery: (queryTerms, params, socket) => {
        const bucketName = params.bucketName;
        indexd.get(bucketName, (err, data) => {
            if (err) {
                return ;
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
            queryTerms = queryTerms[0].toString(':').split(':');
            queryTerms = queryTerms.map(function (elem) {
                return data.objNametoBitPos.mapping[elem];
            });
            socket.write(JSON.stringify({op: 2, params, result: queryTerms}));
        });
    },

    constructResponse: (result, params) => {
        let { bucketName, prefix, marker, maxKeys, delimiter, cb} = params;
        client.listObject(bucketName, { prefix, marker, maxKeys, delimiter },
            logger, (err, data) => {
                if (err) {
                    return cb(err);
                }
                data.Contents = data.Contents.filter(elem => {
                    return result.indexOf(elem.key) !== -1;
                });
                return cb(err, data);
            });
    },

    initIndex: (bucketName) => {
        const data = { objNametoBitPos: {nextAvail: [], length:1, mapping:{}}, acl: {}, contentType: {},
        modificationDate: {}, fileSize: {}, XAmzMeta: {} };
        indexd.put(bucketName, JSON.stringify(data), err =>{
            if (err) {
                return ;
            }
            return ;
        })
    },

    updateIndex: (bucketName, objName, objVal) => {
        indexd.get(bucketName, (err, data) => {
            if (err) {
                return ;
            }
            data = JSON.parse(data);
            let rowId = 0;
            if (Object.keys(data.objNametoBitPos)) {
                if (typeof data.objNametoBitPos.mapping[objName] === 'number') {
                    rowId = data.objNametoBitPos.mapping[objName];
                } else if (data.objNametoBitPos.nextAvail.length > 0) {
                    rowId = data.objNametoBitPos.nextAvail[0];
                    data.objNametoBitPos.nextAvail.splice(0, 1);
                }
                else {
                    rowId = data.objNametoBitPos.length;
                    data.objNametoBitPos.length += 1;
                }
            }
            data.objNametoBitPos.mapping[rowId] = objName;
            data.objNametoBitPos.mapping[objName] = rowId;
            if (config.systemMetaIndexing) {
                const fileSizeDt = objVal['content-length'];
                const contentType = objVal['content-type'];
                updateFileSizeIndex(data.fileSize, fileSizeDt, rowId);
                updatemodDateIndex(data.modificationDate, objVal['last-modified'], rowId);
                updateContentTypeIndex(data.contentType, contentType, rowId);
                updateACLIndex(data.acl, objVal.acl, rowId);
            }
            if (config.userMetaIndexing) {
                updateXAmzMetaIndex(data.XAmzMeta, objVal, rowId);
            }
            indexd.put(bucketName, JSON.stringify(data), err => {
                if (err) {
                    return ;
                }
                return ;
            });
        });
    },

    deleteEntry: (bucketName, objName) => {
        indexd.get(bucketName, (err, data) => {
            if (err) {
                return ;
            }
            data = JSON.parse(data);
            const rowId = data.objNametoBitPos.mapping[objName];
            delete data.objNametoBitPos.mapping[objName];
            delete data.objNametoBitPos.mapping[rowId];
            data.objNametoBitPos.nextAvail.push(rowId);
            indexd.put(bucketName, JSON.stringify(data), err => {
                if (err) {
                    return ;
                }
                return ;
            });
        });
    },
};

export default index;
