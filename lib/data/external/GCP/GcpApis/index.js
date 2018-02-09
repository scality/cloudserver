module.exports = {
    // JSON functions
    copyObject: require('./copyObject'),
    updateMetadata: require('./updateMetadata'),
    deleteObjects: require('./deleteObjects'),
    rewriteObject: require('./rewriteObject'),
    // mpu functions
    abortMultipartUpload: require('./abortMPU'),
    completeMultipartUpload: require('./completeMPU'),
    createMultipartUpload: require('./createMPU'),
    listParts: require('./listParts'),
    uploadPart: require('./uploadPart'),
    uploadPartCopy: require('./uploadPartCopy'),
};
