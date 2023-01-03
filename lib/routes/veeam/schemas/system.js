const joi = require('joi');
const { errors } = require('arsenal');

// Allow supporting any version of the protocol
const systemSchemasPerVersion = {
    'unsupported': joi.object({}),
    '"1.0"': joi.object({
        SystemInfo: joi.object({
            ProtocolVersion: joi.string().required(),
            ModelName: joi.string().required(),
            ProtocolCapabilities: joi.object({
                CapacityInfo: joi.boolean().required(),
                UploadSessions: joi.boolean().required(),
                IAMSTS: joi.boolean().default(false),
            }).required(),
            APIEndpoints: joi.object({
                IAMEndpoint: joi.string().required(),
                STSEndpoint: joi.string().required()
            }),
            SystemRecommendations: joi.object({
                S3ConcurrentTaskLimit: joi.number().min(0).default(64),
                S3MultiObjectDeleteLimit: joi.number().min(1).default(1000),
                StorageCurrentTasksLimit: joi.number().min(0).default(0),
                KbBlockSize: joi.number()
                    .valid(256, 512, 1024, 2048, 4096, 8192)
                    .default(1024),
            }),
        }).required()
    }),
};

/**
 * Validates and parse the provided JSON object from the
 * provided XML file. XML scheme example:
 *
 * <?xml version="1.0" encoding="utf-8" ?>
 * <SystemInfo>
 *     <ProtocolVersion>"1.0"</ProtocolVersion>
 *     <ModelName>"ACME corp - Custom S3 server - v1.2"</ModelName>
 *     <ProtocolCapabilities>
 *         <CapacityInfo>true</CapacityInfo>
 *         <UploadSessions>true</UploadSessions>
 *         <IAMSTS>true</IAMSTS>
 *     </ProtocolCapabilities>
 *     <APIEndpoints>
 *         <IAMEndpoint>https://storage.acme.local/iam/endpoint</IAMEndpoint>
 *         <STSEndpoint>https://storage.acme.local/sts/endpoint</STSEndpoint>
 *     </APIEndpoints>
 *     <SystemRecommendations>
 *         <S3ConcurrentTaskLimit>64</S3ConcurrentTaskLimit>
 *         <S3MultiObjectDeleteLimit>1000</S3MultiObjectDeleteLimit>
 *         <StorageCurrentTaksLimit>0</StorageCurrentTaskLimit>
 *         <KbBlockSize>1024</KbBlockSize>
 *     </SystemRecommendations>
 * </SystemInfo>
 *
 * @param {string} parsedXML - the parsed XML from xml2js
 * @returns {object | Error} the valid system.xml JS object or an error if
 * validation fails
 */
function validateSystemSchema(parsedXML) {
    const protocolVersion = parsedXML?.SystemInfo?.ProtocolVersion;
    let schema = systemSchemasPerVersion.unsupported;
    if (!protocolVersion) {
        throw new Error(errors.MalformedXML
            .customizeDescription('ProtocolVersion must be set for the system.xml file'));
    }
    if (protocolVersion && protocolVersion in systemSchemasPerVersion) {
        schema = systemSchemasPerVersion[parsedXML?.SystemInfo?.ProtocolVersion];
    }
    const validatedData = schema.validate(parsedXML, {
        // Allow any unknown keys for future compatibility
        allowUnknown: true,
        convert: true,
    });
    if (validatedData.error) {
        throw validatedData.error;
    } else {
        switch (protocolVersion) {
            case '"1.0"':
                // Ensure conditional fields are set
                // IAMSTS === true implies that SystemInfo.APIEndpoints is defined
                if (validatedData.value.SystemInfo.ProtocolCapabilities.IAMSTS
                    && !validatedData.value.SystemInfo.APIEndpoints) {
                    throw new Error(errors.MalformedXML);
                }
                break;
            default:
                break;
        }
    }
    return validatedData.value;
}

module.exports = validateSystemSchema;
