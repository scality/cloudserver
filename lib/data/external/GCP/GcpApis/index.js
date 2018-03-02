module.exports = {
    // mpu functions
    abortMultipartUpload: require('./abortMPU'),
    completeMultipartUpload: require('./completeMPU'),
    createMultipartUpload: require('./createMPU'),
    listParts: require('./listParts'),
    uploadPart: require('./uploadPart'),
    uploadPartCopy: require('./uploadPartCopy'),
    // object tagging
    putObject: require('./putObject'),
    putObjectTagging: require('./putTagging'),
    getObjectTagging: require('./getTagging'),
    deleteObjectTagging: require('./deleteTagging'),
};
