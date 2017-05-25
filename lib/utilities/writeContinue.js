/**
 * writeContinue - Handles sending a HTTP/1.1 100 Continue message to the
 * client, indicating that the request body should be sent.
 * @param {object} req - http request object
 * @param {object} res - http response object
 * @param {string} apiMethod - The API method to call
 * @return {undefined}
 */
function writeContinue(req, res) {
    const { headers } = req;
    // The Expect field-value is case-insensitive. Thus, just check for the
    // Expect header's presence and a valid field-value.
    if (headers.expect && headers.expect.toLowerCase() === '100-continue') {
        res.writeContinue();
    }
    return undefined;
}

module.exports = writeContinue;
