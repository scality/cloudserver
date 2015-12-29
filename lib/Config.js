import assert from 'assert';
import fs from 'fs';
import path from 'path';

/**
 * Reads from a config file and returns the content as a config object
 */
export default class Config {
    constructor() {
        /*
         * By default, the config file is "config.json" at the root.
         * It can be overridden using the S3_CONFIG_FILE environment var.
         */
        this.path = path.join(__dirname, '../config.json');
        if (process.env.S3_CONFIG_FILE !== undefined) {
            this.path = process.env.S3_CONFIG_FILE;
        }

        // Read config automatically
        this._getConfig();
    }

    _getConfig() {
        let config;
        try {
            const data = fs.readFileSync(this.path, { encoding: 'utf-8' });
            config = JSON.parse(data);
        } catch (err) {
            throw new Error('could not parse config file: ' + err.message);
        }

        this.port = 8000;
        if (config.port !== undefined) {
            assert(Number.isInteger(config.port) && config.port > 0,
                   'bad config: port must be a positive integer');
            this.port = config.port;
        }

        /*
         * Splitter is used to build the object name for the overview of a
         * multipart upload and to build the object names for each part of a
         * multipart upload.  These objects with large names are then stored in
         * metadata in a "shadow bucket" to a real bucket.  The shadow bucket
         * contains all ongoing multipart uploads.  We include in the object
         * name all of the info we might need to pull about an open multipart
         * upload or about an individual part with each piece of info separated
         * by the splitter.  We can then extract each piece of info by splitting
         * the object name string with this splitter.
         * For instance, the name of the upload overview would be:
         *   overview...!*!objectKey...!*!uploadId...!*!destinationBucketName
         *   ...!*!initiatorID...!*!initiatorDisplayName...!*!ownerID
         *   ...!*!ownerDisplayName...!*!storageClass...!*!timeInitiated
         * For instance, the name of a part would be:
         *   uploadId...!*!partNumber...!*!
         *   timeLastModified...!*!ETag...!*!size...!*!location
         *
         * The sequence of characters used in the splitter should not occur
         * elsewhere in the pieces of info to avoid splitting where not
         * intended.
         */

        // TODO: Determine a splitter that is DNS compliant and will
        // not cause an issue for multipartUpload.  This splitter
        // will work for serviceGet.  This is GH Issue#218.
        this.splitter = 'splitterfornow';
        if (config.splitter !== undefined) {
            assert(typeof config.splitter === 'string',
                   'bad config: splitter must be a string');
            this.splitter = config.splitter;
        }

        // TODO: Move namespace setting from utils.js to
        // this config file and base the name of the user bucket
        // on the namespace.  This is GH Issue#216
        // Note: The first character is a "." because
        // AWS does not allow bucketnames to start with "."
        this.usersBucket = `namespaceusersbucket`;
        if (config.splitter !== undefined) {
            assert(typeof config.usersBucket === 'string',
                   'bad config: usersBucket must be a string');
            this.usersBucket = config.usersBucket;
        }

        this.clustering = false;
        if (config.clustering !== undefined) {
            assert(typeof config.clustering === 'boolean',
                   'bad config: clustering must be either true or false');
            this.clustering = config.clustering;
        }

        this.clusters = false;
        if (config.clusters !== undefined) {
            assert(Number.isInteger(config.clusters) && config.clusters > 0,
                   'bad config: clusters must be a positive integer');
            this.clusters = config.clusters;
        }

        this.sproxyd = { bootstrap: [] };
        if (config.sproxyd !== undefined
                && config.sproxyd.bootstrap !== undefined) {
            assert(config.sproxyd.bootstrap instanceof Array
                   && config.sproxyd.bootstrap.every(
                       e => typeof e === 'string'),
                   'bad config: sproxyd.bootstrap must be a list of strings');
            this.sproxyd.bootstrap = config.sproxyd.bootstrap;
        }

        this.bucketd = { bootstrap: [] };
        if (config.bucketd !== undefined
                && config.bucketd.bootstrap !== undefined) {
            assert(config.bucketd.bootstrap instanceof Array
                   && config.bucketd.bootstrap.every(
                       e => typeof e === 'string'),
                   'bad config: bucketd.bootstrap must be a list of strings');
            this.bucketd.bootstrap = config.bucketd.bootstrap;
        }

        this.log = { logLevel: 'debug', dumpLevel: 'error', logstash: {} };
        if (config.log !== undefined) {
            if (config.log.logLevel !== undefined) {
                assert(typeof config.log.logLevel === 'string',
                        'bad config: log.logLevel must be a string');
            }
            if (config.log.dumpLevel !== undefined) {
                assert(typeof config.log.dumpLevel === 'string',
                        'bad config: log.dumpLevel must be a string');
            }
            if (config.log.logstash !== undefined) {
                assert(config.log.logstash instanceof Object
                    && (config.log.logstash.host !== undefined ||
                        config.log.logstash.port !== undefined),
                        'bad config: log.logstash.host and log.logstash.port ' +
                            'must be defined');
            }
        }
    }
}
