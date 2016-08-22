import BucketClientInterface from '../metadata/bucketclient/backend';
import BucketFileInterface from '../metadata/bucketfile/backend';
import BucketInfo from '../metadata/BucketInfo';
import inMemory from '../metadata/in_memory/backend';
import config from '../Config';
import async from 'async';
import bitmap from 'node-bitmap-ewah';

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

const index = {
    processQueryHeader: (header) => {
        if (!header)
            return header;
        const queryTerms = header.split("&");
        const query = [];
        for (let i=0; i<queryTerms.length; i++) {
            if (queryTerms[i].indexOf("op/NOT") === -1)
                query.push(queryTerms[i]);
            else {
                query.push(queryTerms[i]+"&"+queryTerms[i+1]);
                i+=1;
            }
        }
        return query;
    },

    evaluateQuery: (queryTerms, bucketName, log, cb, params) => {
        async.map(queryTerms, getObjects.bind(null, {bucketName, log, cb}), function(err, query){
            while(query.length > 1) {
                let operatorPos = -1;
                for(let i = query.length-1; i>=0; i--) {
                    if (query[i] === "op/AND" || query[i] === "op/OR") {
                        operatorPos = i;
                        break;
                    }
                }
                if (query[operatorPos] === "op/AND")
                    query.splice(operatorPos, 3, query[operatorPos+1].and(query[operatorPos+2]));
                else if (query[operatorPos] === "op/OR")
                    query.splice(operatorPos, 3, query[operatorPos+1].or(query[operatorPos+2]));
            }
        constructResponse(query[0], bucketName, log, cb, params);
        });
    },

    initRowIds: (bucketName, log, cb) => {
        client.putObject(bucketName, "I|objNameToRowID", JSON.stringify({}), log, err => {
            if (err)
                return cb(err);
            client.putObject(bucketName, "I|x-amz-meta", JSON.stringify({counter:0}), log, err => {
                if (err)
                    return cb(err);
                client.putObject(bucketName, "I|fileSize", JSON.stringify({counter:0}), log, err => {
                    if (err)
                        return cb(err);
                    client.putObject(bucketName, "I|modificationDate", JSON.stringify({counter:0}), log, err => {
                        if (err)
                            return cb(err);
                        client.putObject(bucketName, "I|contentType", JSON.stringify({counter:0}), log, err => {
                            if (err)
                                return cb(err);
                            return cb(err);
                        });
                    });
                });
            });
        });
    },

    updateRowIds: (bucketName, objName, objVal, log, cb) => {
        if (objName.indexOf("..|..") !== -1)
            return cb(null);
        client.getObject(bucketName, "I|objNameToRowID", log, (err, data) => {
            if (err)
                return cb(err);
            data = JSON.parse(data);
            let rowId = 0;
            if (Object.keys(data)) {
                if (typeof data[objName] === "number")
                    rowId = data[objName];
                else
                    rowId = Object.keys(data).length/2+1;
            }
            data[rowId] = objName;
            data[objName] = rowId;
            client.putObject(bucketName, "I|objNameToRowID", JSON.stringify(data), log, err => {
                if (err)
                    return cb(err);
                if (config.systemMetaIndexing)
                    return updateContentTypeIndex(bucketName, objName, objVal, rowId, Object.keys(data).length/2, log, cb)
                else if (config.userMetaIndexing)
                    return updateXAmzMetaIndex(bucketName, objName, objVal, rowId, Object.keys(data).length/2, log, cb);
            });
        });
    }

};

export default index;

function updateContentTypeIndex(bucketName, objName, objVal, rowId, objCounter, log, cb) {
    client.getObject(bucketName, "I|contentType", log, (err, data) => {
        if (err)
            return cb(err);
        data = JSON.parse(data);
        const typeTag = "contenttype/"+objVal['content-type'].split("/")[0];
        const subtypeTag = "contentsubtype/"+objVal['content-type'].split("/")[1];
        data[typeTag] = updateBitmap(storeToBitmap(data[typeTag]), rowId, objCounter);
        if (subtypeTag)
            data[subtypeTag] = updateBitmap(storeToBitmap(data[subtypeTag]), rowId, objCounter);
        if (rowId > data.counter)
            data.counter = rowId;
        client.putObject(bucketName, "I|contentType", JSON.stringify(data), log, err => {
            if (err)
                return cb(err);
            return updatemodificationDateIndex(bucketName, objName, objVal, rowId, objCounter, log, cb);
        });
    });
}

function updatemodificationDateIndex(bucketName, objName, objVal, rowId, objCounter, log, cb) {
    client.getObject(bucketName, "I|modificationDate", log, (err, data) => {
        if (err)
            return cb(err);
        data = JSON.parse(data);
        const date = new Date(objVal.Date);
        data = updateIntegerIndex("modificationdate-year/"+date.getUTCFullYear(), rowId, objCounter, data);
        data = updateIntegerIndex("modificationdate-month/"+(date.getUTCMonth()+1), rowId, objCounter, data);
        data = updateIntegerIndex("modificationdate-day/"+date.getUTCDate(), rowId, objCounter, data);
        data = updateIntegerIndex("modificationdate-hours/"+date.getUTCHours(), rowId, objCounter, data);
        data = updateIntegerIndex("modificationdate-minutes/"+date.getUTCMinutes(), rowId, objCounter, data);
        if (rowId > data.counter)
            data.counter = rowId;
        client.putObject(bucketName, "I|modificationDate", JSON.stringify(data), log, err => {
            if (err) {
                return cb(err);
            }
            return updateFileSizeIndex(bucketName, objName, objVal, rowId, objCounter, log, cb);
        });
    });
}

function updateFileSizeIndex(bucketName, objName, objVal, rowId, objCounter, log, cb) {
    client.getObject(bucketName, "I|fileSize", log, (err, data) => {
        if (err) {
            return cb(err);
        }
        data = JSON.parse(data);
        const tag = "filesize/"+objVal['content-length'];
        data = updateIntegerIndex(tag, rowId, objCounter, data);
        if (rowId > data.counter)
            data.counter = rowId;
        client.putObject(bucketName, "I|fileSize", JSON.stringify(data), log, err => {
            if (err) {
                return cb(err);
            }
            if (config.userMetaIndexing)
                return updateXAmzMetaIndex(bucketName, objName, objVal, rowId, objCounter, log, cb);
            else
                return cb(err);
        });
    });
}

function updateXAmzMetaIndex(bucketName, objName, objVal, rowId, objCounter, log, cb) {
    const tags = [];
    Object.keys(objVal).forEach(elem => {
        if (elem.indexOf("x-amz-meta") != -1 && elem != "x-amz-meta-s3cmd-attrs")
            tags.push(elem+"/"+objVal[elem]);
    });
    if (tags.length === 0)
        return cb(null);
    client.getObject(bucketName, "I|x-amz-meta", log, (err, data) => {
        if (err)
            return cb(err);
        data = JSON.parse(data);
        tags.forEach(tag => {
            if (tag.indexOf("--integer") !== -1) {
                tag = tag.replace("--integer", "");
                data = updateIntegerIndex(tag, rowId, objCounter, data);
            }
            else
                data[tag] = updateBitmap(storeToBitmap(data[tag]), rowId, objCounter);
        });
        if (rowId > data.counter)
            data.counter = rowId;
        client.putObject(bucketName, "I|x-amz-meta", JSON.stringify(data), log, err => {
            if (err)
                return cb(err);
            return cb(err);
        });
    });
}

function updateIntegerIndex(tag, rowId, objCounter, indexData) {
    const tagAttr = tag.split("/")[0];
    const tagValue = parseInt(tag.split("/")[1]);
    if (!indexData.hasOwnProperty(tagAttr))
        indexData[tagAttr] = [];
    if (indexData[tagAttr].indexOf(tagValue) === -1)
        indexData[tagAttr].push(tagValue);
    indexData[tagAttr].sort();
    const ind = indexData[tagAttr].indexOf(tagValue);
    let index = {};
    if (!indexData.hasOwnProperty(tagAttr+"/"+indexData[tagAttr][ind+1]))
        indexData[tag] = updateBitmap(storeToBitmap(indexData[tag]), rowId, objCounter);
    else
        indexData[tag] = updateBitmap(storeToBitmap(indexData[tagAttr+"/"+indexData[tagAttr][ind+1]]), rowId, objCounter);
    for (var i=ind-1; i>=0; i--) {
        index = storeToBitmap(indexData[tagAttr+"/"+indexData[tagAttr][i]]);
        index.push(rowId);
        indexData[tagAttr+"/"+indexData[tagAttr][i]] = bitmapToStore(index);
    }
    return indexData;
}

function getObjects(params, searchTerm, callback) {
    if (searchTerm .indexOf("op/AND") !==-1 || searchTerm.indexOf("op/OR") !==-1)
        return callback(null, searchTerm);
    let notOperator = false;
    if (searchTerm.indexOf("op/NOT") !== -1) {
        searchTerm = searchTerm.split("&")[1];
        notOperator = true;
    }
    if (searchTerm.indexOf("x-amz-meta") !== -1)
        getObjectsfromXAmzMeta(searchTerm, notOperator, params, callback);
    else if (searchTerm.indexOf("filesize") !== -1)
        getObjectsfromFileSize(searchTerm, notOperator, params, callback);
    else if (searchTerm.indexOf("modificationdate") !== -1)
        getObjectsfromModificationDate(searchTerm, notOperator, params, callback);
    else if (searchTerm.indexOf("contenttype") !== -1)
        getObjectsfromContentType(searchTerm, notOperator, params, callback);
}

function getObjectsfromContentType(searchTerm, notOperator, params, callback) {
    const { bucketName, log, cb } = params;
    const typeTerm = searchTerm.split("/")[0]+"/"+searchTerm.split("/")[1];
    const subtypeTerm = searchTerm.split("/")[0]+"/"+searchTerm.split("/")[2];
    client.getObject(bucketName, "I|contentType", log, (err, data) => {
        if (err)
            return cb(err);
        data = JSON.parse(data);
        let result = storeToBitmap(data[typeTerm]);
        if (searchTerm.split("/") > 2)
            result = result.and(storeToBitmap(data[subtypeTerm]));
        if (notOperator) {
            result.push(data.counter+1);
            result = result.not();
        }
        callback(null, result);
    });
}


function getObjectsfromModificationDate(searchTerm, notOperator, params, callback) {
    const { bucketName, log, cb } = params;
    const operator = searchTerm.split("/")[1];
    const year = searchTerm.split("/")[2];
    const month = searchTerm.split("/")[3];
    const day = searchTerm.split("/")[4];
    const hours = searchTerm.split("/")[5];
    const minutes = searchTerm.split("/")[6];
    client.getObject(bucketName, "I|modificationDate", log, (err, data) => {
        if (err)
            return cb(err);
        data = JSON.parse(data);
        const yearEq = evaluateIntegerRange("=", "modificationdate-year/"+year, data);
        const monthEq = evaluateIntegerRange("=", "modificationdate-month/"+month, data);
        const dayEq = evaluateIntegerRange("=", "modificationdate-day/"+day, data);
        const hoursEq = evaluateIntegerRange("=", "modificationdate-hours/"+hours, data);
        const minutesEq = evaluateIntegerRange("=", "modificationdate-minutes/"+minutes, data);
        let result;
        if (operator === "=") {
            result = yearEq;
            if (month)
                result = result.and(monthEq);
            if (day)
                result = result.and(dayEq);
            if (hours)
                result = result.and(hoursEq);
            if (minutes)
                result = result.and(minutesEq);
        }
        else if (operator.indexOf("<") !== -1 || operator.indexOf(">") !== -1) {
            result = evaluateIntegerRange(operator, "modificationdate-year/"+year, data);
            if (month) {
                const tmp = evaluateIntegerRange(operator, "modificationdate-month/"+month, data);
                result = result.or(yearEq.and(tmp));
            }
            if (day) {
                const tmp = evaluateIntegerRange(operator, "modificationdate-day/"+day, data);
                result = result.or(yearEq.and(monthEq.and(tmp)));
            }
            if (hours) {
                const tmp = evaluateIntegerRange(operator, "modificationdate-hours/"+hours, data);
                result = result.or(yearEq.and(monthEq.and(dayEq.and(tmp))));
            }
        }
        if (notOperator) {
            result.push(data.counter+1);
            result = result.not();
        }
        callback(null, result);
    });
}

function getObjectsfromFileSize(searchTerm, notOperator, params, callback) {
    const { bucketName, log, cb } = params;
    const term = searchTerm.split("/")[0]+"/"+searchTerm.split("/")[2];
    const operator = searchTerm.split("/")[1];
    client.getObject(bucketName, "I|fileSize", log, (err, data) => {
        if (err)
            return cb(err);
        data = JSON.parse(data);
        let result = evaluateIntegerRange(operator, term, data);
        if (notOperator) {
            result.push(data.counter+1);
            result = result.not();
        }
        callback(null, result);
    });
}

function getObjectsfromXAmzMeta(searchTerm, notOperator, params, callback) {
    const { bucketName, log, cb } = params;
    let term = null;
    let operator = null;
    if (searchTerm.indexOf("--integer") !== -1) {
        term = searchTerm.split("/")[0]+"/"+searchTerm.split("/")[2];
        operator = searchTerm.split("/")[1];
    }
    else
        term = searchTerm;
    client.getObject(bucketName, "I|x-amz-meta", log, (err, data) => {
        if (err)
            return cb(err);
        data = JSON.parse(data);
        let result = null;
        if (operator)
            result = evaluateIntegerRange(operator, term, data);
        else
            result = storeToBitmap(data[term]);
        if (notOperator) {
            result.push(data.counter+1);
            result = result.not();
        }
        callback(null, result);
    });
}

function findNearestValue(array, start, end, value, operator) {
    const mid = Math.floor((start+end)/2);
    if (array[mid] === value)
        return [mid, mid];
    if (value < array[0])
        return [-1,0];
    if (value > array[array.length-1])
        return [array.length-1, array.length];
    if (end-start<=1)
        return [start, end];
    else if (array[mid] < value)
        return findNearestValue(array, mid, end, value, operator);
    else
        return findNearestValue(array, start, mid, value, operator);
}

function evaluateIntegerRange(operator, term, data) {
    term = term.replace("--integer", "");
    const attr = term.split("/")[0];
    const value = parseInt(term.split("/")[1]);
    if (value === NaN)
        return bitmap.createObject();
    const nearestValuePos = findNearestValue(data[attr], 0, data[attr].length-1, value, operator);
    let result = bitmap.createObject();
    if (operator.indexOf("=") !== -1) {
        const nextValue = term.split("/")[0]+"/"+data[attr][data[attr].indexOf(value)+1];
        if (!data.hasOwnProperty(term))
            result = result.or(bitmap.createObject());
        else
            result = result.or(storeToBitmap(data[term]).xor(storeToBitmap(data[nextValue])));
    }
    if (operator.indexOf("<") !== -1) {
        const lowestValue = term.split("/")[0]+"/"+data[attr][0];
        const nearestValue = term.split("/")[0]+"/"+ data[attr][nearestValuePos[1]];
        if (nearestValuePos[0] === -1)
            result = result.or(bitmap.createObject());
        else if (nearestValuePos[1] === data[attr].length)
            result = result.or(storeToBitmap(data[lowestValue]));
        else
            result = result.or(storeToBitmap(data[nearestValue]).xor(storeToBitmap(data[lowestValue])));
    }
    if (operator.indexOf(">") !== -1) {
        const lowestValue = term.split("/")[0]+"/"+data[attr][0];
        const nearestValue = term.split("/")[0]+"/"+ data[attr][nearestValuePos[0]];
        const nextValue = term.split("/")[0]+"/"+data[attr][nearestValuePos[0]+1];
        if (nearestValuePos[0] === -1)
            result = result.or(storeToBitmap(data[lowestValue]));
        else if (nearestValuePos[1] === data[attr].length || term === nextValue)
            result = result.or(bitmap.createObject());
        else
            result = result.or(storeToBitmap(data[nearestValue]).and(storeToBitmap(data[nextValue])));
    }
    return result;
}

function constructResponse(result, bucketName, log, cb, params) {
    const { prefix, marker, delimiter, maxKeys } = params;
    result = result.toString(":").split(":");
    client.getObject(bucketName, "I|objNameToRowID", log, (err, data) => {
        if (err)
            return cb(err);
        data = JSON.parse(data);
        result = result.map(function(elem){
            return data[elem];
        });
        client.listObject(bucketName, { prefix:"", marker, maxKeys, delimiter },
            log, (err, data) => {
                if (err)
                    return cb(err);
                data.Contents = data.Contents.filter(function(elem) {
                    return result.indexOf(elem.key) !== -1;
                });
                return cb(err, data);
            });
    });
}

function updateBitmap(bitmap, rowId, objCounter) {
    if (rowId === objCounter)
        bitmap.push(rowId);
    else
        bitmap = bitmap.copyandset(rowId);
    return bitmapToStore(bitmap);
}

function storeToBitmap(stored) {
    const bm = bitmap.createObject();
    if (stored) {
        stored[2] = new Buffer(stored[2], "binary");
        bm.read(stored);
    }
    return bm;
}

function bitmapToStore(bitmap) {
    let toStore = bitmap.write();
    toStore[2] = toStore[2].toString("binary");
    return toStore;
}
