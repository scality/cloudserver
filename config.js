const config = {};

// Splitter is used to build the object name for the
// overview of a multipart upload and to build the
// object names for each part of a multipart upload.
// These objects with large names are then stored in
// metadata in a "shadow bucket" to a real bucket.
// The shadow bucket contains all ongoing multipart uploads.
// We include in the object name all
// of the info we might need to pull about an open
// multipart upload or about an individual part with each
// piece of info separated by the splitter.
// We can then extract each piece of info by splitting
// the object name string with this splitter.
// For instance, the name of the upload overview would be:
// overview...!*!objectKey...!*!uploadId...!*!destinationBucketName
// ...!*!initiatorID...!*!initiatorDisplayName...!*!ownerID
// ...!*!ownerDisplayName...!*!storageClass...!*!timeInitiated
// For instance, the name of a part would be:
// uploadId...!*!partNumber...!*!
// timeLastModified...!*!etag...!*!size...!*!location
//
// The sequence of characters used in teh splitter should
// not occur elsewhere in the pieces of info to avoid splitting
// where not intended.
config.splitter = '...!*!';

export default config;
