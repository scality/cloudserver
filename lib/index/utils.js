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
        client.getObject(bucketName, "I|bitmapIndex", log, (err, data) => {
            if (err)
                return cb(err);
            data = JSON.parse(data);
            queryTerms = queryTerms.map(function(searchTerm){
                if (searchTerm .indexOf("op/AND") !==-1 || searchTerm.indexOf("op/OR") !==-1)
                    return searchTerm;
                let notOperator = false;
                if (searchTerm.indexOf("op/NOT") !== -1) {
                    searchTerm = searchTerm.split("&")[1];
                    notOperator = true;
                }
                let result;
                if (searchTerm.indexOf("x-amz-meta") !== -1)
                    result = getObjectsfromXAmzMeta(data.x_amz_meta, searchTerm);
                else if (searchTerm.indexOf("filesize") !== -1)
                    result = getObjectsfromFileSize(data.fileSize, searchTerm);
                else if (searchTerm.indexOf("modificationdate") !== -1)
                    result = getObjectsfromModificationDate(data.modificationDate, searchTerm);
                else if (searchTerm.indexOf("contenttype") !== -1)
                    result = getObjectsfromContentType(data.contentType, searchTerm);
                else if (searchTerm.indexOf("acl") !== -1)
                    result = getObjectsfromACL(data.acl, searchTerm);
                if (notOperator) {
                    result.push(data.counter+1);
                    result = result.not();
                }
                return result;
            });
            while(queryTerms.length > 1) {
                let operatorPos = -1;
                for(let i = queryTerms.length-1; i>=0; i--) {
                    if (queryTerms[i] === "op/AND" || queryTerms[i] === "op/OR") {
                        operatorPos = i;
                        break;
                    }
                }
                if (queryTerms[operatorPos] === "op/AND")
                    queryTerms.splice(operatorPos, 3, queryTerms[operatorPos+1].and(queryTerms[operatorPos+2]));
                else if (queryTerms[operatorPos] === "op/OR")
                    queryTerms.splice(operatorPos, 3, queryTerms[operatorPos+1].or(queryTerms[operatorPos+2]));
            }
            constructResponse(data.objNameToRowID, queryTerms[0], bucketName, log, cb, params);
        });
    },

    initRowIds: (bucketName, log, cb) => {
        const data = { objNameToRowID:{}, acl:{} , contentType:{}, modificationDate:{}, fileSize:{}, x_amz_meta:{} };
        client.putObject(bucketName, "I|bitmapIndex", JSON.stringify(data), log, err => {
            if (err)
                return cb(err);
            return cb(err);
        });
    },

    updateRowIds: (bucketName, objName, objVal, log, cb) => {
        if (objName.indexOf("..|..") !== -1)
            return cb(null);
        client.getObject(bucketName, "I|bitmapIndex", log, (err, data) => {
            if (err)
                return cb(err);
            data = JSON.parse(data);
            let rowId = 0;
            if (Object.keys(data.objNameToRowID)) {
                if (typeof data.objNameToRowID[objName] === "number")
                    rowId = data.objNameToRowID[objName];
                else
                    rowId = Object.keys(data.objNameToRowID).length/2+1;
            }
            data.objNameToRowID[rowId] = objName;
            data.objNameToRowID[objName] = rowId;
            const objCounter = Object.keys(data.objNameToRowID).length/2;
            if (config.systemMetaIndexing) {
                data.fileSize = updateFileSizeIndex(data.fileSize, objVal['content-length'], rowId, objCounter);
                data.modificationDate = updatemodificationDateIndex(data.modificationDate, objVal.Date, rowId, objCounter);
                data.contentType = updateContentTypeIndex(data.contentType, objVal['content-type'], rowId, objCounter);
                data.acl = updateACLIndex(data.acl, objVal.acl, rowId, objCounter);
            }
            if (config.userMetaIndexing)
                data.x_amz_meta = updateXAmzMetaIndex(data.x_amz_meta, objVal, rowId, objCounter);

            client.putObject(bucketName, "I|bitmapIndex", JSON.stringify(data), log, err => {
                if (err)
                    return cb(err);
                return cb(err);
            });
        });
    }

};

export default index;

function deleteOldEntries(rowId, objCounter, data) {
    Object.keys(data).forEach(key => {
        if (typeof data[key] === "object" && key !== "counter") {
            let tmp = storeToBitmap(data[key]);
            tmp.unset(rowId);
            data[key] = bitmapToStore(tmp);
        }
    });
    return data;
}

function updateACLIndex(data, objVal, rowId, objCounter) {
    data = deleteOldEntries(rowId, objCounter, data);
    Object.keys(objVal).forEach(elem =>{
        if (typeof objVal[elem] === "string") {
            const tag = "acl-"+elem+"/"+objVal[elem]
                data[tag] = updateBitmap(storeToBitmap(data[tag]), rowId, objCounter);
        }
        else {
            objVal[elem].forEach(item =>{
                const tag = "acl-"+elem+"/"+item
                data[tag] = updateBitmap(storeToBitmap(data[tag]), rowId, objCounter);
            })
        }
    });
    return data;
}

function updateContentTypeIndex(data, objVal, rowId, objCounter) {
    const typeTag = "contenttype/"+objVal.split("/")[0];
    const subtypeTag = "contentsubtype/"+objVal.split("/")[1];
    data[typeTag] = updateBitmap(storeToBitmap(data[typeTag]), rowId, objCounter);
    if (subtypeTag)
        data[subtypeTag] = updateBitmap(storeToBitmap(data[subtypeTag]), rowId, objCounter);
    return data;
}

function updatemodificationDateIndex(data, objVal, rowId, objCounter) {
    const date = new Date(objVal);
    data = updateIntegerIndex("modificationdate-year/"+date.getUTCFullYear(), rowId, objCounter, data);
    data = updateIntegerIndex("modificationdate-month/"+(date.getUTCMonth()+1), rowId, objCounter, data);
    data = updateIntegerIndex("modificationdate-day/"+date.getUTCDate(), rowId, objCounter, data);
    data = updateIntegerIndex("modificationdate-hours/"+date.getUTCHours(), rowId, objCounter, data);
    data = updateIntegerIndex("modificationdate-minutes/"+date.getUTCMinutes(), rowId, objCounter, data);
    return data;
}

function updateFileSizeIndex(data, objVal, rowId, objCounter) {
    const tag = "filesize/"+objVal;
    data = updateIntegerIndex(tag, rowId, objCounter, data);
    return data;
}

function updateXAmzMetaIndex(data, objVal, rowId, objCounter) {
    const tags = [];
    Object.keys(objVal).forEach(elem => {
        if (elem.indexOf("x-amz-meta") != -1 && elem != "x-amz-meta-s3cmd-attrs")
            tags.push(elem+"/"+objVal[elem]);
    });
    if (tags.length === 0)
        return data;
    tags.forEach(tag => {
        if (tag.indexOf("--integer") !== -1) {
            tag = tag.replace("--integer", "");
            data = updateIntegerIndex(tag, rowId, objCounter, data);
        }
        else {
            data[tag] = updateBitmap(storeToBitmap(data[tag]), rowId, objCounter);
        }
    });
    return data;
}

function updateIntegerIndex(tag, rowId, objCounter, indexData) {
    const tagAttr = tag.split("/")[0];
    const tagValue = parseInt(tag.split("/")[1]);
    if (!indexData.hasOwnProperty(tagAttr))
        indexData[tagAttr] = [];
    if (indexData[tagAttr].indexOf(tagValue) === -1)
        indexData[tagAttr].push(tagValue);
    indexData[tagAttr].sort(function (a, b) {  return a - b;  });
    const ind = indexData[tagAttr].indexOf(tagValue);
    let index = {};
    if (indexData.hasOwnProperty(tagAttr+"/"+indexData[tagAttr][ind+1]) && !indexData.hasOwnProperty(tag))
        indexData[tag] = updateBitmap(storeToBitmap(indexData[tagAttr+"/"+indexData[tagAttr][ind+1]]), rowId, objCounter);
    else
        indexData[tag] = updateBitmap(storeToBitmap(indexData[tag]), rowId, objCounter);
    for (var i=ind-1; i>=0; i--) {
        index = storeToBitmap(indexData[tagAttr+"/"+indexData[tagAttr][i]]);
        index.push(rowId);
        indexData[tagAttr+"/"+indexData[tagAttr][i]] = bitmapToStore(index);
    }
    return indexData;
}

function getObjectsfromACL(data, searchTerm) {
    const term = searchTerm.split("/")[0]+"-"+searchTerm.split("/")[1]+"/"+searchTerm.split("/")[2];
    return storeToBitmap(data[term]);
}

function getObjectsfromContentType(data, searchTerm) {
    const typeTerm = searchTerm.split("/")[0]+"/"+searchTerm.split("/")[1];
    const subtypeTerm = searchTerm.split("/")[0]+"/"+searchTerm.split("/")[2];
    let result = storeToBitmap(data[typeTerm]);
    if (searchTerm.split("/") > 2)
        result = result.and(storeToBitmap(data[subtypeTerm]));
    return result;
}

function getObjectsfromModificationDate(data, searchTerm) {
    const operator = searchTerm.split("/")[1];
    const year = searchTerm.split("/")[2];
    const month = searchTerm.split("/")[3];
    const day = searchTerm.split("/")[4];
    const hours = searchTerm.split("/")[5];
    const minutes = searchTerm.split("/")[6];
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
    return result;
}

function getObjectsfromFileSize(data, searchTerm) {
    const term = searchTerm.split("/")[0]+"/"+searchTerm.split("/")[2];
    const operator = searchTerm.split("/")[1];
    return evaluateIntegerRange(operator, term, data);
}

function getObjectsfromXAmzMeta(data, searchTerm) {
    let term = null;
    let operator = null;
    if (searchTerm.indexOf("--integer") !== -1) {
        term = searchTerm.split("/")[0]+"/"+searchTerm.split("/")[2];
        operator = searchTerm.split("/")[1];
    }
    else
        term = searchTerm;
    if (operator)
        return evaluateIntegerRange(operator, term, data);
    else
        return storeToBitmap(data[term]);
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

function constructResponse(data, result, bucketName, log, cb, params) {
    const { prefix, marker, delimiter, maxKeys } = params;
    result = result.toString(":").split(":");
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
}

function updateBitmap(bitmap, rowId, objCounter) {
    if(bitmap.length()-1 <= rowId)
        bitmap.push(rowId);
    else
        bitmap.set(rowId);
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
