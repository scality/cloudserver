const { maximumAllowedPartCount } = require('../../../../../constants');

const canonicalObjectConfig = {
    bucket: 'mpu-test-bucket-canonical-object',
    object: 'mpu-test-object-canonical',
    bodySize: 1024 * 1024 * 5,
    bodyContent: 'a',
    howManyParts: 3,
    partNumbers: Array.from(Array(3).keys()), // 3 corresponds to howManyParts
    invalidPartNumbers: [-1, 0, maximumAllowedPartCount + 1],
    signature: 'for canonical object',
    meta: {
        computeTotalSize: (partNumbers, bodySize) => partNumbers.reduce((total, current) =>
            total + bodySize + current + 1
        , 0),
        objectIsEmpty: false,
    },
};

const emptyObjectConfig = {
    bucket: 'mpu-test-bucket-empty-object',
    object: 'mpu-test-object-empty',
    bodySize: 0,
    bodyContent: null,
    howManyParts: 1,
    partNumbers: Array.from(Array(1).keys()), // 1 corresponds to howManyParts
    invalidPartNumbers: [-1, 0, maximumAllowedPartCount + 1],
    signature: 'for empty object',
    meta: {
        computeTotalSize: () => 0,
        objectIsEmpty: true,
    },
};

const objectConfigs = [
    canonicalObjectConfig,
    emptyObjectConfig,
];

module.exports = objectConfigs;
