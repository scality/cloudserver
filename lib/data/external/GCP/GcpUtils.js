const werelogs = require('werelogs');
const { errors, s3middleware } = require('arsenal');

const _config = require('../../../Config').config;
const { gcpTaggingPrefix } = require('../../../../constants');

werelogs.configure({
    level: _config.log.logLevel,
    dump: _config.log.dumpLevel,
});

const logger = new werelogs.Logger('gcpUtil');

function eachSlice(size) {
    this.array = [];
    let partNumber = 1;
    for (let ind = 0; ind < this.length; ind += size) {
        this.array.push({
            Parts: this.slice(ind, ind + size),
            PartNumber: partNumber++,
        });
    }
    return this.array;
}

function getSourceInfo(CopySource) {
    const source =
        CopySource.startsWith('/') ? CopySource.slice(1) : CopySource;
    const sourceArray = source.split(/\/(.+)/);
    const sourceBucket = sourceArray[0];
    const sourceObject = sourceArray[1];
    return { sourceBucket, sourceObject };
}

function getPaddedPartNumber(number) {
    return `000000${number}`.substr(-5);
}

function getPartNumber(number) {
    if (isNaN(number)) {
        return undefined;
    }
    if (typeof number === 'string') {
        return parseInt(number, 10);
    }
    return number;
}

function createMpuKey(key, uploadId, partNumberArg, fileNameArg) {
    let partNumber = partNumberArg;
    let fileName = fileNameArg;

    if (typeof partNumber === 'string' && fileName === undefined) {
        fileName = partNumber;
        partNumber = null;
    }
    const paddedNumber = getPaddedPartNumber(partNumber);
    if (fileName && typeof fileName === 'string') {
        // if partNumber is given, return a "full file path"
        // else return a "directory path"
        return partNumber ? `${key}-${uploadId}/${fileName}/${paddedNumber}` :
            `${key}-${uploadId}/${fileName}`;
    }
    if (partNumber && typeof partNumber === 'number') {
        // filename wasn't passed as an argument. Create default
        return `${key}-${uploadId}/parts/${paddedNumber}`;
    }
    // returns a "directory path"
    return `${key}-${uploadId}/`;
}

function createMpuList(params, level, size) {
    // populate and return a parts list for compose
    const retList = [];
    for (let i = 1; i <= size; ++i) {
        retList.push({
            PartName: createMpuKey(params.Key, params.UploadId, i, level),
            PartNumber: i,
        });
    }
    return retList;
}

function processTagSet(tagSet = []) {
    if (tagSet.length > 10) {
        return errors.BadRequest
            .customizeDescription('Object tags cannot be greater than 10');
    }
    let error = undefined;
    const tagAsMeta = {};
    const taggingDict = {};
    tagSet.every(tag => {
        const { Key: key, Value: value } = tag;
        if (key.length > 128) {
            error = errors.InvalidTag
                .customizeDescription(
                    `The TagKey provided is too long, ${key.length}`);
            return false;
        }
        if (value.length > 256) {
            error = errors.InvalidTag
                .customizeDescription(
                    `The TagValue provided is too long, ${value.length}`);
            return false;
        }
        if (taggingDict[key]) {
            error = errors.InvalidTag
                .customizeDescription(
                    'Cannot provide multiple Tags with the same key');
            return false;
        }
        tagAsMeta[`${gcpTaggingPrefix}${key}`] = value;
        taggingDict[key] = true;
        return true;
    });
    if (error) {
        return error;
    }
    return tagAsMeta;
}

function stripTags(metadata = {}) {
    const retMD = Object.assign({}, metadata);
    Object.keys(retMD).forEach(key => {
        if (key.startsWith(gcpTaggingPrefix)) {
            delete retMD[key];
        }
    });
    return retMD;
}

function retrieveTags(metadata = {}) {
    const retTagSet = [];
    Object.keys(metadata).forEach(key => {
        if (key.startsWith(gcpTaggingPrefix)) {
            retTagSet.push({
                Key: key.slice(gcpTaggingPrefix.length),
                Value: metadata[key],
            });
        }
    });
    return retTagSet;
}

function getPutTagsMetadata(metadata, tagging = '') {
    let retMetadata = metadata || {};
    retMetadata = stripTags(retMetadata);
    const tagObj = s3middleware.tagging.parseTagFromQuery(tagging);
    Object.keys(tagObj).forEach(header => {
        const prefixed = `${gcpTaggingPrefix}${header}`.toLowerCase();
        retMetadata[prefixed] = tagObj[header];
    });
    return retMetadata;
}

module.exports = {
    // functions
    eachSlice,
    createMpuKey,
    createMpuList,
    getSourceInfo,
    processTagSet,
    stripTags,
    retrieveTags,
    getPutTagsMetadata,
    getPartNumber,
    // util objects
    logger,
};
