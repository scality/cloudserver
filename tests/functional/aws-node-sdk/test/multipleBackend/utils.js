
const azure = require('azure-storage');

const { config } = require('../../../../../lib/Config');

const azureLocation = 'azuretest';

const utils = {};

utils.uniqName = name => `${name}${new Date().getTime()}`;

utils.getAzureClient = () => {
    let isTestingAzure;
    let azureBlobEndpoint;
    let azureBlobSAS;
    let azureClient;
    if (process.env[`${azureLocation}_AZURE_BLOB_ENDPOINT`]) {
        isTestingAzure = true;
        azureBlobEndpoint = process.env[`${azureLocation}_AZURE_BLOB_ENDPOINT`];
    } else if (config.locationConstraints[azureLocation] &&
          config.locationConstraints[azureLocation].details &&
          config.locationConstraints[azureLocation].details.azureBlobEndpoint) {
        isTestingAzure = true;
        azureBlobEndpoint =
          config.locationConstraints[azureLocation].details.azureBlobEndpoint;
    } else {
        isTestingAzure = false;
    }

    if (isTestingAzure) {
        if (process.env[`${azureLocation}_AZURE_BLOB_SAS`]) {
            azureBlobSAS = process.env[`${azureLocation}_AZURE_BLOB_SAS`];
            isTestingAzure = true;
        } else if (config.locationConstraints[azureLocation] &&
            config.locationConstraints[azureLocation].details &&
            config.locationConstraints[azureLocation].details.azureBlobSAS
        ) {
            azureBlobSAS = config.locationConstraints[azureLocation].details
              .azureBlobSAS;
            isTestingAzure = true;
        } else {
            isTestingAzure = false;
        }
    }

    if (isTestingAzure) {
        azureClient = azure.createBlobServiceWithSas(azureBlobEndpoint,
          azureBlobSAS);
    }
    return azureClient;
};

utils.getAzureContainerName = () => {
    let azureContainerName;
    if (config.locationConstraints[azureLocation] &&
    config.locationConstraints[azureLocation].details &&
    config.locationConstraints[azureLocation].details.azureContainerName) {
        azureContainerName =
          config.locationConstraints[azureLocation].details.azureContainerName;
    }
    return azureContainerName;
};

utils.getAzureKeys = () => {
    const keys = [
        {
            describe: 'empty',
            name: `somekey-${Date.now()}`,
            body: '',
            MD5: 'd41d8cd98f00b204e9800998ecf8427e',
        },
        {
            describe: 'normal',
            name: `somekey-${Date.now()}`,
            body: Buffer.from('I am a body', 'utf8'),
            MD5: 'be747eb4b75517bf6b3cf7c5fbb62f3a',
        },
        {
            describe: 'big',
            name: `bigkey-${Date.now()}`,
            body: new Buffer(10485760),
            MD5: 'f1c9645dbc14efddc7d8a322685f26eb',
        },
    ];
    return keys;
};

// For contentMD5, Azure requires base64 but AWS requires hex, so convert
// from base64 to hex
utils.convertMD5 = contentMD5 =>
    Buffer.from(contentMD5, 'base64').toString('hex');

module.exports = utils;
