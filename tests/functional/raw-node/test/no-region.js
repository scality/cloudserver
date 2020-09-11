const assert = require('assert');
const { makeS3Request } = require('../utils/makeRequest');
const itSkipIfAWS = process.env.AWS_ON_AIR ? it.skip : it;


describe('Requests without region specified', () => {
    const queryObj = {};
    const bucket = 'foo';
    itSkipIfAWS('should return a response when region is not specified', cb => {
        makeS3Request({ method: 'GET', queryObj, bucket }, (err, res) => {
            console.log(err, res);
            cb();
        });
    });
});
