const { config } = require('../../../../../lib/Config');

const isCEPH = process.env.CI_CEPH !== undefined;
const isS3C = process.env.S3_END_TO_END !== undefined;
const itSkipCeph = isCEPH ? it.skip : it;
const itSkipS3C = isS3C ? it.skip : it;
const itSkipNotS3C = isS3C ? it : it.skip;
const itSkipCephS3C = isCEPH || isS3C ? it.skip : it;

const describeSkipIfCeph = isCEPH ? describe.skip : describe.skip; // always skip
const describeSkipIfAWS = process.env.AWS_ON_AIR ? describe.skip : describe;
const describeSkipIfS3C = isS3C ? describe.skip : describe;

let describeSkipIfNotMultiple = describe.skip;
let describeSkipIfNotMultipleOrCeph = describe.skip;

if (config.backends.data === 'multiple') {
    describeSkipIfNotMultiple = describe;
    describeSkipIfNotMultipleOrCeph = isCEPH ? describe.skip : describe.skip; // always skip
}

module.exports = {
    isCEPH,
    isS3C,
    itSkipCeph,
    itSkipS3C,
    itSkipNotS3C,
    itSkipCephS3C,
    describeSkipIfCeph,
    describeSkipIfNotMultiple,
    describeSkipIfNotMultipleOrCeph,
    describeSkipIfAWS,
    describeSkipIfS3C,
};
