// VersionID format:
//         timestamp  sequential_position  site_id  other_information
// where:
// - timestamp              14 bytes        epoch in ms (good untill 5138)
// - sequential_position    06 bytes        position in the ms slot (1B ops)
// - site_id                05 bytes        site identifier (like PARIS)
// - other_information      arbitrary       user input, such as a unique string

// the lengths of the components in bytes
const LENGTH_TS = 14; // timestamp: epoch in ms
const LENGTH_SQ = 6; // position in ms slot
const LENGTH_ST = 5; // site identifier

// empty string template for the variables in a versionId
const TEMPLATE_TS = new Array(LENGTH_TS + 1).join('0');
const TEMPLATE_SQ = new Array(LENGTH_SQ + 1).join('0');
const TEMPLATE_ST = new Array(LENGTH_ST + 1).join(' ');

// site identifier, like PARIS, TOKYO; will be trimmed if exceeding max length
const SITE_ID = `${process.env.SITE_ID}${TEMPLATE_ST}`.slice(0, LENGTH_ST);

// constants for max epoch and max sequential number in the same epoch
const MAX_TS = Math.pow(10, LENGTH_TS) - 1; // good until 16 Nov 5138
const MAX_SQ = Math.pow(10, LENGTH_SQ) - 1; // good for 1 billion ops

// the earliest versionId, used for versions before versioning
const VID_INF = `${TEMPLATE_TS}${MAX_TS}`.slice(-LENGTH_TS) +
                `${TEMPLATE_SQ}${MAX_SQ}`.slice(-LENGTH_SQ) + SITE_ID;

// internal state of the module
let prvts = 0; // epoch of the last versionId
let prvsq = 0; // sequential number of the last versionId

/**
 * This function ACTIVELY (wastes CPU cycles and) waits for an amount of time
 * before returning to the caller. This should not be used frequently.
 *
 * @param {Number} span - time to wait in nanoseconds (1/1000000 millisecond)
 * @return {Undefined} - nothing
 */
function wait(span) {
    function getspan(diff) {
        return diff[0] * 1e9 + diff[1];
    }
    const start = process.hrtime();
    while (getspan(process.hrtime(start)) < span);
}

/**
 * This function returns a "versionId" string indicating the current time as a
 * combination of the current time in millisecond, the position of the request
 * in that millisecond, and the identifier of the local site (which could be
 * datacenter, region, or server depending on the notion of geographics). This
 * function is stateful which means it keeps some values in the memory and the
 * next call depends on the previous call.
 *
 * @param {string} info - the additional info to ensure uniqueness if desired
 * @return {string} - the formated versionId string
 */
function generateVersionId(info) {
    // Need to wait for the millisecond slot got "flushed". We wait for
    // only a single millisecond when the module is restarted, which is
    // necessary for the correctness of the system. This is therefore cheap.
    if (prvts === 0) {
        wait(1000000);
    }
    // get the present epoch (in millisecond)
    const ts = Date.now();
    // A bit more rationale: why do we use a sequence number instead of using
    // process.hrtime which gives us time in nanoseconds? The idea is that at
    // any time resolution, some concurrent requests may have the same time due
    // to the way the OS is queueing requests or getting clock cycles. Our
    // approach however will give the time based on the position of a request
    // in the queue for the same millisecond which is supposed to be unique.

    // increase the position if this request is in the same epoch
    prvsq = (prvts === ts) ? prvsq + 1 : 0;
    prvts = ts;

    // In the default cases, we reverse the chronological order of the
    // timestamps so that all versions of an object can be retrieved in the
    // reversed chronological order---newest versions first. This is because of
    // the limitation of leveldb for listing keys in the reverse order.
    return `${TEMPLATE_TS}${MAX_TS - prvts}`.slice(-LENGTH_TS) +
        `${TEMPLATE_SQ}${MAX_SQ - prvsq}`.slice(-LENGTH_SQ) + SITE_ID + info;
}

module.exports = { generateVersionId, VID_INF };
