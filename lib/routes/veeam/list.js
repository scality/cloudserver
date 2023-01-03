const url = require('url');
const xml2js = require('xml2js');
const { errors } = require('arsenal');
const querystring = require('querystring');
const metadata = require('../../metadata/wrapper');
const { responseXMLBody } = require('arsenal/build/lib/s3routes/routesUtils');
const { respondWithData, getResponseHeader, buildHeadXML, validPath } = require('./utils');

function buildContent(arrayOfFiles) {
    const baseXML = [];
    arrayOfFiles.forEach(file => {
        baseXML.push('<Contents>');
        baseXML.push(`<Key>${file.name}</Key>`);
        baseXML.push(`<LastModified>${file['Last-Modified']}</LastModified>`);
        baseXML.push(`<ETag>${file.ETag}</ETag>`);
        baseXML.push(`<Size>${file['Content-Length']}</Size>`);
        baseXML.push('<StorageClass>STANDARD</StorageClass>');
        baseXML.push('</Contents>');
    });
    return baseXML.join('');
}

function buildVersionContent(arrayOfFiles) {
    const baseXML = [];
    arrayOfFiles.forEach(file => {
        baseXML.push('<Version>');
        baseXML.push(`<Key>${file.name}</Key>`);
        baseXML.push('<VersionId>null</VersionId>');
        baseXML.push('<IsLatest>true</IsLatest>');
        baseXML.push(`<LastModified>${file['Last-Modified']}</LastModified>`);
        baseXML.push(`<ETag>${file.ETag}</ETag>`);
        baseXML.push(`<Size>${file['Content-Length']}</Size>`);
        baseXML.push('<StorageClass>STANDARD</StorageClass>');
        baseXML.push('</Version>');
    });
    return baseXML.join('');
}

/**
 * Utility function to build a versioned response for the LIST route.
 * It adds the supported path by default as a static and default file.
 *
 * @param {object} request - request object
 * @param {object} arrayOfFiles - array of files headers
 * @returns {string} - the formatted XML content to send
 */
function buildXMLVersionedResponseForListing(request, arrayOfFiles) {
    const parsedUrl = url.parse(request.url);
    const parsedQs = querystring.parse(parsedUrl.query);
    return `<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Name>${request.bucketName}</Name>
    <Prefix>${validPath}</Prefix>
    <KeyCount>${(arrayOfFiles.length + 1) || 1}</KeyCount>
    <MaxKeys>${parsedQs['max-keys'] || 1000}</MaxKeys>
    <Delimiter>/</Delimiter>
    <IsTruncated>false</IsTruncated>
	<Version>
		<Key>${validPath}</Key>
		<VersionId>null</VersionId>
		<LastModified>${new Date().toISOString()}</LastModified>
		<ETag>"d41d8cd98f00b204e9800998ecf8427e"</ETag>
		<IsLatest>true</IsLatest>
		<Size>0</Size>
		<StorageClass>STANDARD</StorageClass>
	</Version>
    ${buildVersionContent(arrayOfFiles)}
</ListVersionsResult>`;
}

/**
 * Utility function to build a standard response for the LIST route.
 * It adds the supported path by default as a static and default file.
 *
 * @param {object} request - request object
 * @param {object} arrayOfFiles - array of files headers
 * @returns {string} - the formatted XML content to send
 */
function buildXMLResponseForListing(request, arrayOfFiles) {
    const parsedUrl = url.parse(request.url);
    const parsedQs = querystring.parse(parsedUrl.query);
    return `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <Name>${request.bucketName}</Name>
    <Prefix>${validPath}</Prefix>
    <KeyCount>${(arrayOfFiles.length + 1) || 1}</KeyCount>
    <MaxKeys>${parsedQs['max-keys'] || 1000}</MaxKeys>
    <Delimiter>/</Delimiter>
    <IsTruncated>false</IsTruncated>
	<Contents>
		<Key>${validPath}</Key>
		<LastModified>${new Date().toISOString()}</LastModified>
		<ETag>"d41d8cd98f00b204e9800998ecf8427e"</ETag>
		<Size>0</Size>
		<StorageClass>STANDARD</StorageClass>
	</Contents>
    ${buildContent(arrayOfFiles)}
</ListBucketResult>`;
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
                ...getResponseHeader(request, request.bucketName,
                dataBuffer, lastModified, log),
                name: file.name,
            });
        });
        // When `versions` is present, listing should return a versioned list
        if ('versions' in request.query) {
            return respondWithData(request, response, log, request.bucketName,
                buildXMLVersionedResponseForListing(request, filesToBuild));
        } else {
            return respondWithData(request, response, log, request.bucketName,
                buildXMLResponseForListing(request, filesToBuild));
        }
    });
}

module.exports = listVeeamFiles;
