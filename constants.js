import crypto from 'crypto';

export default {
    /*
     * Splitter is used to build the object name for the overview of a
     * multipart upload and to build the object names for each part of a
     * multipart upload.  These objects with large names are then stored in
     * metadata in a "shadow bucket" to a real bucket.  The shadow bucket
     * contains all ongoing multipart uploads.  We include in the object
     * name some of the info we might need to pull about an open multipart
     * upload or about an individual part with each piece of info separated
     * by the splitter.  We can then extract each piece of info by splitting
     * the object name string with this splitter.
     * For instance, assuming a splitter of '...!*!',
     * the name of the upload overview would be:
     *   overview...!*!objectKey...!*!uploadId
     * For instance, the name of a part would be:
     *   uploadId...!*!partNumber
     *
     * The sequence of characters used in the splitter should not occur
     * elsewhere in the pieces of info to avoid splitting where not
     * intended.
     *
     * Splitter is also used in adding bucketnames to the
     * namespacerusersbucket.  The object names added to the
     * namespaceusersbucket are of the form:
     * canonicalID...!*!bucketname
     */

    splitter: '..|..',
    // BACKWARD: This line will be removed when removing backward compatibility
    oldSplitter: 'splitterfornow',
    usersBucket: 'users..bucket',
    oldUsersBucket: 'namespaceusersbucket',
    // MPU Bucket Prefix is used to create the name of the shadow
    // bucket used for multipart uploads.  There is one shadow mpu
    // bucket per bucket and its name is the mpuBucketPrefix followed
    // by the name of the final destination bucket for the object
    // once the multipart upload is complete.
    mpuBucketPrefix: 'mpuShadowBucket',
    // PublicId is used as the canonicalID for a request that contains
    // no authentication information.  Requestor can access
    // only public resources
    publicId: 'http://acs.amazonaws.com/groups/global/AllUsers',
    // All Authenticated Users is an ACL group.
    allAuthedUsersId: 'http://acs.amazonaws.com/groups/' +
        'global/AuthenticatedUsers',
    // LogId is used for the AWS logger to write the logs
    // to the destination bucket.  This style of logging is
    // to be implemented later but the logId is used in the
    // ACLs.
    logId: 'http://acs.amazonaws.com/groups/s3/LogDelivery',
    emptyFileMd5: 'd41d8cd98f00b204e9800998ecf8427e',

    // Number of sub-directories for file backend
    folderHash: 3511, // Prime number
    // AWS sets a hard limit on the listing maxKeys
    // http://docs.aws.amazon.com/AmazonS3/latest/API/
    //      RESTBucketGET.html#RESTBucketGET-requests
    listingHardLimit: 1000,
    // Metadata for storage system topology
    // A topology is generated automatically in init.js and saved in
    // `./${topology_name}.json`
    // A raw topology is saved in `./${topology_name}.raw.json`
    topology: {
        mem: {
            // name of file containing the topology
            name: `${__dirname}/topologyMem`,
            // data placement type determines which type of fragments stored in
            //  this backend, either 'none', 'data', 'parity', 'both'
            dp: 'both',
            id: 'm',
            md: [{
                // domain name, e.g 'Rack'
                domain: '',
                // number of children components
                number: 0,
                // weight of component, either a number or [min, max]. A random
                //  weight is drawn within the given range in the latter case.
                weight: 1,
                // true -> a component can contain multiple fragments of an
                //  object,
                // false otherwise.
                replacement: true,
                // range of bits extracted from DISPERSION part of fragment's
                //  key to determine location of the fragment
                binImgRange: [0, 24],
            }],
        },
        file: {
            name: `${__dirname}/topologyFile`,
            dp: 'both',
            id: 'f',
            // multiple domaines are defined in a sequential way,
            // i.e. every prior domain will contain posterior domains
            md: [{
                domain: 'Rack',
                number: 20,
                replacement: false,
                binImgRange: [8, 24],
            }, {
                domain: 'Server',
                number: 2,
                replacement: false,
                binImgRange: [0, 24],
            }],
        },
        scality: {
            name: `${__dirname}/topologyScality`,
            dp: 'both',
            id: 's',
            // keyFrag can be defined as either `key` or `suffix` to indicate
            // that data backend generate or update a suffix to frag's key
            keyFrag: 'key',
            md: [{
                domain: '',
                number: 0,
                weight: 1,
                replacement: true,
                binImgRange: [0, 24],
            }],
        },
    },
    // erasure codes
    ec: {
        // default erasure coding
        params: {
            bc_id: 1,   // eslint-disable-line
            k: 12,       // number of data fragments
            m: 6,       // number of parity fragments
            w: 8,
            hd: 7,
            ct: 2,
        },
        // available erasure codes backends
        backendId: {
            EC_BACKEND_NULL: 0,
            EC_BACKEND_JERASURE_RS_VAND: 1,
            EC_BACKEND_JERASURE_RS_CAUCHY: 2,
            EC_BACKEND_FLAT_XOR_HD: 3,
            EC_BACKEND_ISA_L_RS_VAND: 4,
            EC_BACKEND_SHSS: 5,
            EC_BACKEND_LIBERASURECODE_RS_VAND: 6,
            EC_BACKENDS_MAX: 99,
        },
        // available checksum types
        checksumType: {
            CHKSUM_NONE: 1,
            CHKSUM_CRC32: 2,
            CHKSUM_MD5: 3,
            CHKSUM_TYPES_MAX: 99,
        },
        // dimension of Galois field
        gfDim: {
            RS: 8,
            XOR: 1,
        },
    },
    // AWS sets a minimum size limit for parts except for the last part.
    // http://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadComplete.html
    minimumAllowedPartSize: 5242880,

    // hex digest of sha256 hash of empty string:
    emptyStringHash: crypto.createHash('sha256')
        .update('', 'binary').digest('hex'),
};
