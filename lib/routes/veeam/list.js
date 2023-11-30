const url = require('url');
const xml2js = require('xml2js');
const { errors } = require('arsenal');
const querystring = require('querystring');
const metadata = require('../../metadata/wrapper');
const { responseXMLBody } = require('arsenal/build/lib/s3routes/routesUtils');
const { respondWithData, getResponseHeader, buildHeadXML, validPath } = require('./utils');
const { processVersions, processMasterVersions } = require('../../api/bucketGet');


/**
 * Utility function to build a standard response for the LIST route.
 * It adds the supported path by default as a static and default file.
 *
 * @param {object} request - request object
 * @param {object} arrayOfFiles - array of files headers
 * @param {boolean} [versioned] - set to true if versioned listing is enabled
 * @returns {string} - the formatted XML content to send
 */
function buildXMLResponse(request, arrayOfFiles, versioned = false) {
    const parsedUrl = url.parse(request.url);
    const parsedQs = querystring.parse(parsedUrl.query);

    const listParams = {
        prefix: validPath,
        maxKeys: parsedQs['max-keys'] || 1000,
        delimiter: '/',
    };
    const list = {
        IsTruncated: false,
        Versions: [],
        Contents: [],
        CommonPrefixes: [],
    };
    const entries = arrayOfFiles.map(file => ({
        key: file.name,
        value: {
            IsDeleteMarker: false,
            IsNull: true,
            LastModified: file['Last-Modified'],
            // Generated ETag alrady contains quotes, removing them here
            ETag: file.ETag.substring(1, file.ETag.length - 1),
            Size: file['Content-Length'],
            Owner: {
                ID: 0,
                DisplayName: 'Veeam SOSAPI',
            },
            StorageClass: 'VIRTUAL',
        }
    }));
    entries.push({
        key: validPath,
        value: {
            IsDeleteMarker: false,
            IsNull: true,
            LastModified: new Date().toISOString(),
            ETag: 'd41d8cd98f00b204e9800998ecf8427e',
            Size: 0,
            Owner: {
                ID: 0,
                DisplayName: 'Veeam SOSAPI',
            },
            StorageClass: 'VIRTUAL',
        }
    });
    // Add the folder as the base file
    if (versioned) {
        list.Versions = entries;
    } else {
        list.Contents = entries;
    }
    const processingXMLFunction = versioned ? processVersions : processMasterVersions;
    return processingXMLFunction(request.bucketName, listParams, list);
}

/**
 * List system.xml and/or capacity.xml files for a given bucket.
 *
 * @param {object} request - request object
 * @param {object} response - response object
 * @param {object} bucketMd - bucket metadata from the db
 * @param {object} log - logger object
 * @returns {undefined} -
 */
function listVeeamFiles(request, response, bucketMd, log) {
    if (!bucketMd) {
        return responseXMLBody(errors.NoSuchBucket, null, response, log);
    }
    // Only accept list-type query parameter
    if (!('list-type' in request.query) && !('versions' in request.query)) {
        return responseXMLBody(errors.InvalidRequest
            .customizeDescription('The Veeam folder does not support this action.'), null, response, log);
    }
    return metadata.getBucket(request.bucketName, log, (err, data) => {
        if (err) {
            return responseXMLBody(errors.InternalError, null, response, log);
        }
        const filesToBuild = [];
        const fieldsToGenerate = [];
        if (data._capabilities?.VeeamSOSApi?.SystemInfo) {
            fieldsToGenerate.push({
                ...data._capabilities?.VeeamSOSApi?.SystemInfo,
                name: `${validPath}system.xml`,
            });
        }
        if (data._capabilities?.VeeamSOSApi?.CapacityInfo) {
            fieldsToGenerate.push({
                ...data._capabilities?.VeeamSOSApi?.CapacityInfo,
                name: `${validPath}capacity.xml`,
            });
        }
        fieldsToGenerate.forEach(file => {
            const lastModified = file.LastModified;
            // eslint-disable-next-line no-param-reassign
            delete file.LastModified;
            const builder = new xml2js.Builder({
                headless: true,
            });
            const dataBuffer = Buffer.from(buildHeadXML(builder.buildObject(file)));
            filesToBuild.push({
                ...getResponseHeader(request, data,
                    dataBuffer, lastModified, log),
                name: file.name,
            });
        });
        // When `versions` is present, listing should return a versioned list
        return respondWithData(request, response, log, data,
            buildXMLResponse(request, filesToBuild, 'versions' in request.query));
    });
}

module.exports = listVeeamFiles;
