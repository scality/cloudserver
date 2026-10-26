/**
 * Destroy the decipher streams prepared upfront by objectGet when they are not
 * all consumed: objectGet error, error response or interrupted streaming.
 * They hold native OpenSSL memory that is better released than left to GC.
 * @param {Error|null} err - error returned by objectGet
 * @param {object} request - request, with the encrypted locations set by objectGet
 * @param {http.ServerResponse} response - response sent to the client
 * @return {undefined}
 */
function destroyDecipherStreamsOnError(err, request, response) {
    // only set by objectGet when upfront KMS calls are not skipped
    const { kmsDecipherLocations } = request;
    if (!kmsDecipherLocations) {
        return;
    }
    const destroyDecipherStreams = () => kmsDecipherLocations.forEach(loc => {
        if (loc.decipherStream) {
            loc.decipherStream.destroy();
        }
    });
    // isclosed is set by the server when the client closed before objectGet returned
    if (err || response.isclosed) {
        destroyDecipherStreams();
        return;
    }
    response.once('close', () => {
        // a successful response fully sent has consumed every decipher stream
        if (!response.writableFinished || response.statusCode >= 400) {
            destroyDecipherStreams();
        }
    });
}

module.exports = destroyDecipherStreamsOnError;
