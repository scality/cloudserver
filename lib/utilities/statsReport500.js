/**
* Report 500 to stats when an Internal Error occurs
* @param {object} err - Arsenal error
* @param {object} statsClient - StatsClient instance
* @returns {undefined}
*/
export default function statsReport500(err, statsClient) {
    if (err && err.code === 500) {
        statsClient.report500();
    }
    return undefined;
}
