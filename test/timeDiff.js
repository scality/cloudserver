module.exports = function (startTime) {
    var timeArray = process.hrtime(startTime);
    // timeArray[0] is whole seconds and timeArray[1] is remaining nanoseconds
    var milliseconds = (timeArray[0] * 1000) + (timeArray[1] / 1e6);
    return milliseconds;
};
