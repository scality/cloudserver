const { getPutTagsMetadata } = require('../GcpUtils');

function putObject(params, callback) {
    const putParams = Object.assign({}, params);
    putParams.Metadata = getPutTagsMetadata(putParams.Metadata, params.Tagging);
    delete putParams.Tagging;
    // error handling will be by the actual putObject request
    return this.putObjectReq(putParams, callback);
}

module.exports = putObject;
