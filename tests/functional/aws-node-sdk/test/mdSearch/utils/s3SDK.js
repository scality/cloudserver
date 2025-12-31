const { S3Client } = require('@aws-sdk/client-s3');
const getConfig = require('../../support/config');

module.exports = new S3Client(getConfig('default'));
