import assert from 'assert';
import fs from 'fs';
import path from 'path';
import { ipCheck } from 'arsenal';

import authDataChecker from './auth/in_memory/checker';

// whitelist IP, CIDR for health checks
const defaultHealthChecks = { allowFrom: ['127.0.0.1/8', '::1'] };

const defaultLocalCache = { host: '127.0.0.1', port: 6379 };

function sproxydAssert(configSproxyd, cb) {
    if (configSproxyd.bootstrap !== undefined) {
        assert(Array.isArray(configSproxyd.bootstrap)
            && configSproxyd.bootstrap
                .every(e => typeof e === 'string'),
            'bad config: sproxyd.bootstrap must be a list of strings');
        assert(configSproxyd.bootstrap.length > 0,
                'sproxyd bootstrap list is empty');
        // thisSproxyd.bootstrap = configSproxyd.bootstrap;
        cb('bootstrap');
    }
    if (configSproxyd.chordCos !== undefined) {
        assert(typeof configSproxyd.chordCos === 'string',
            'bad config: sproxyd.chordCos must be a string');
        assert(configSproxyd.chordCos.match(/^[0-9a-fA-F]{2}$/),
            'bad config: sproxyd.chordCos must be a 2hex-chars string');
        // thisSproxyd.chordCos =
            // Number.parseInt(configSproxyd.chordCos, 16);
        cb('chordCos');
    }
}
/**
 * Reads from a config file and returns the content as a config object
 */
class Config {
    constructor() {
        /*
         * By default, the config file is "config.json" at the root.
         * It can be overridden using the S3_CONFIG_FILE environment var.
         */
        this._basePath = path.join(__dirname, '..');
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
            throw new Error(`could not parse config file: ${err.message}`);
        }

        this.port = 8000;
        if (config.port !== undefined) {
            assert(Number.isInteger(config.port) && config.port > 0,
                'bad config: port must be a positive integer');
            this.port = config.port;
        }

        this.listenOn = [];
        if (config.listenOn !== undefined) {
            assert(Array.isArray(config.listenOn)
                && config.listenOn.every(e => typeof e === 'string'),
                'bad config: listenOn must be a list of strings');
            config.listenOn.forEach(item => {
                const lastColon = item.lastIndexOf(':');
                // if address is IPv6 format, it includes brackets
                // that have to be removed from the final IP address
                const ipAddress = item.indexOf(']') > 0 ?
                    item.substr(1, lastColon - 2) :
                    item.substr(0, lastColon);
                // the port should not include the colon
                const port = item.substr(lastColon + 1);
                // parseIp returns as empty object if the address is invalid
                assert(Object.keys(ipCheck.parseIp(ipAddress)).length !== 0,
                    'bad config: listenOn IP address must be valid');
                assert(parseInt(port, 10),
                    'bad config: listenOn port must be a positive integer');
                this.listenOn.push({ ip: ipAddress, port });
            });
        }

        // legacy
        if (config.regions !== undefined) {
            assert(typeof config.regions === 'object',
                'bad config: the list of regions is mandatory');
            assert(Object.keys(config.regions).every(
                r => typeof r === 'string' && config.regions[r] instanceof Array
                && config.regions[r].every(e => typeof e === 'string')),
                'bad config: regions must be a set of {region: [endpoints]}');
            this.regions = config.regions;
        }

        this.locationConstraints = {};
        if (config.locationConstraints !== undefined) {
            assert(typeof config.locationConstraints === 'object',
                'bad config: locationConstraints must be an object');
            Object.keys(config.locationConstraints).forEach(l => {
                assert(typeof config.locationConstraints[l].type === 'string' &&
                    typeof config.locationConstraints[l].information
                        === 'object',
                    'bad config: locationConstraints.type and ' +
                    'locationConstraints.information are mandatory- type ' +
                    'must be a string and information must be an object');
                this.locationConstraints[l] = config.locationConstraints[l];
                this.locationConstraints[l].type =
                    config.locationConstraints[l].type;
                const info = config.locationConstraints[l].information;
                const stringFields = [
                    'region',
                    'bucketName',
                    'credentialsProfile',
                ];
                stringFields.forEach(field => {
                    if (info.field !== undefined) {
                        assert(typeof info.field === 'string',
                            `bad config: ${field} must be a string`);
                        this.locationConstraints[l].information.field =
                            info.field;
                    }
                });
                if (info.connector !== undefined) {
                    assert(typeof info.connector === 'object',
                        'bad config: connector must be an object');
                    if (info.connector.sproxyd !== undefined) {
                        sproxydAssert(info.connector.sproxyd, field => {
                            if (field === 'chordCos') {
                                this.locationConstraints[l].information
                                    .connector.sproxyd[field] =
                                    Number.parseInt(info.connector.
                                        sproxyd[field], 16);
                            } else {
                                this.locationConstraints[l].information
                                    .connector.sproxyd[field] =
                                    info.connector.sproxyd[field];
                            }
                        });
                    }
                }
            });
        }

        this.restEndpoints = {};
        if (config.restEndpoints !== undefined) {
            assert(typeof config.restEndpoints === 'object',
                'bad config: restEndpoints must be an object of endpoints');
            assert(Object.keys(config.restEndpoints).every(
                r => typeof config.restEndpoints[r] === 'string'),
                'bad config: each endpoint must be a string');
            this.restEndpoints = config.restEndpoints;
        }

        this.websiteEndpoints = [];
        if (config.websiteEndpoints !== undefined) {
            assert(Array.isArray(config.websiteEndpoints)
                && config.websiteEndpoints.every(e => typeof e === 'string'),
                'bad config: websiteEndpoints must be a list of strings');
            this.websiteEndpoints = config.websiteEndpoints;
        }

        this.clusters = false;
        if (config.clusters !== undefined) {
            assert(Number.isInteger(config.clusters) && config.clusters > 0,
                   'bad config: clusters must be a positive integer');
            this.clusters = config.clusters;
        }

        this.usEastBehavior = false;
        if (config.usEastBehavior !== undefined) {
            assert(typeof config.usEastBehavior === 'boolean');
            this.usEastBehavior = config.usEastBehavior;
        }
        // legacy
        this.sproxyd = { bootstrap: [] };
        if (config.sproxyd !== undefined) {
            sproxydAssert(config.sproxyd, field => {
                if (field === 'chordCos') {
                    this.sproxyd[field] =
                        Number.parseInt(config.sproxyd[field], 16);
                } else {
                    this.sproxyd[field] = config.sproxyd[field];
                }
            });
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

        this.vaultd = {};
        if (config.vaultd) {
            if (config.vaultd.port !== undefined) {
                assert(Number.isInteger(config.vaultd.port)
                       && config.vaultd.port > 0,
                       'bad config: vaultd port must be a positive integer');
                this.vaultd.port = config.vaultd.port;
            }
            if (config.vaultd.host !== undefined) {
                assert.strictEqual(typeof config.vaultd.host, 'string',
                                   'bad config: vaultd host must be a string');
                this.vaultd.host = config.vaultd.host;
            }
        }

        this.utapi = {};
        if (config.utapi) {
            if (config.utapi.port) {
                assert(Number.isInteger(config.utapi.port)
                    && config.utapi.port > 0,
                    'bad config: utapi port must be a positive integer');
                this.utapi.port = config.utapi.port;
            }
            if (config.utapi.workers !== undefined) {
                assert(Number.isInteger(config.utapi.workers)
                    && config.utapi.workers > 0,
                    'bad config: utapi workers must be a positive integer');
                this.utapi.workers = config.utapi.workers;
            }
            if (config.utapi.redis) {
                if (config.utapi.redis.sentinels) {
                    this.utapi.redis = { sentinels: [], name: null };

                    assert(typeof config.utapi.redis.name === 'string',
                        'bad config: redis sentinel name must be a string');
                    this.utapi.redis.name = config.utapi.redis.name;

                    assert(Array.isArray(config.utapi.redis.sentinels),
                        'bad config: redis sentinels must be an array');
                    config.utapi.redis.sentinels.forEach(item => {
                        const { host, port } = item;
                        assert(typeof host === 'string',
                            'bad config: redis sentinel host must be a string');
                        assert(typeof port === 'number',
                            'bad config: redis sentinel port must be a number');
                        this.utapi.redis.sentinels.push({ host, port });
                    });
                } else {
                    // check for standalone configuration
                    this.utapi.redis = {};
                    assert(typeof config.utapi.redis.host === 'string',
                        'bad config: redis.host must be a string');
                    assert(typeof config.utapi.redis.port === 'number',
                        'bad config: redis.port must be a number');
                    this.utapi.redis.host = config.utapi.redis.host;
                    this.utapi.redis.port = config.utapi.redis.port;
                }
            }
            if (config.utapi.metrics) {
                this.utapi.metrics = config.utapi.metrics;
            }
        }

        this.log = { logLevel: 'debug', dumpLevel: 'error' };
        if (config.log !== undefined) {
            if (config.log.logLevel !== undefined) {
                assert(typeof config.log.logLevel === 'string',
                       'bad config: log.logLevel must be a string');
                this.log.logLevel = config.log.logLevel;
            }
            if (config.log.dumpLevel !== undefined) {
                assert(typeof config.log.dumpLevel === 'string',
                        'bad config: log.dumpLevel must be a string');
                this.log.dumpLevel = config.log.dumpLevel;
            }
        }

        this.kms = {};
        if (config.kms) {
            assert(typeof config.kms.userName === 'string');
            assert(typeof config.kms.password === 'string');
            this.kms.userName = config.kms.userName;
            this.kms.password = config.kms.password;
            if (config.kms.helperProgram !== undefined) {
                assert(typeof config.kms.helperProgram === 'string');
                this.kms.helperProgram = config.kms.helperProgram;
            }
            if (config.kms.propertiesFile !== undefined) {
                assert(typeof config.kms.propertiesFile === 'string');
                this.kms.propertiesFile = config.kms.propertiesFile;
            }
            if (config.kms.maxSessions !== undefined) {
                assert(typeof config.kms.maxSessions === 'number');
                this.kms.maxSessions = config.kms.maxSessions;
            }
        }

        this.healthChecks = defaultHealthChecks;
        if (config.healthChecks && config.healthChecks.allowFrom) {
            assert(config.healthChecks.allowFrom instanceof Array,
                'config: invalid healthcheck configuration. allowFrom must ' +
                'be an array');
            config.healthChecks.allowFrom.forEach(item => {
                assert(typeof item === 'string',
                'config: invalid healthcheck configuration. allowFrom IP ' +
                'address must be a string');
            });
            this.healthChecks.allowFrom = defaultHealthChecks.allowFrom
                .concat(config.healthChecks.allowFrom);
        }

        if (process.env.ENABLE_LOCAL_CACHE) {
            this.localCache = defaultLocalCache;
        }
        if (config.localCache) {
            assert(typeof config.localCache === 'object',
                'config: invalid local cache configuration. localCache must ' +
                'be an object');
            assert(typeof config.localCache.host === 'string',
                'config: invalid host for localCache. host must be a string');
            assert(typeof config.localCache.port === 'number',
                'config: invalid port for localCache. port must be a number');
            this.localCache = {
                host: config.localCache.host,
                port: config.localCache.port,
            };
        }

        if (config.certFilePaths) {
            assert(typeof config.certFilePaths === 'object' &&
                typeof config.certFilePaths.key === 'string' &&
                typeof config.certFilePaths.cert === 'string' && ((
                    config.certFilePaths.ca &&
                    typeof config.certFilePaths.ca === 'string') ||
                    !config.certFilePaths.ca)
               );
        }
        const { key, cert, ca } = config.certFilePaths ?
            config.certFilePaths : {};
        if (key && cert) {
            const keypath = (key[0] === '/') ? key : `${this._basePath}/${key}`;
            const certpath = (cert[0] === '/') ?
                cert : `${this._basePath}/${cert}`;
            let capath = undefined;
            if (ca) {
                capath = (ca[0] === '/') ? ca : `${this._basePath}/${ca}`;
                assert.doesNotThrow(() =>
                    fs.accessSync(capath, fs.F_OK | fs.R_OK),
                    `File not found or unreachable: ${capath}`);
            }
            assert.doesNotThrow(() =>
                fs.accessSync(keypath, fs.F_OK | fs.R_OK),
                `File not found or unreachable: ${keypath}`);
            assert.doesNotThrow(() =>
                fs.accessSync(certpath, fs.F_OK | fs.R_OK),
                `File not found or unreachable: ${certpath}`);
            this.https = {
                cert: fs.readFileSync(certpath, 'ascii'),
                key: fs.readFileSync(keypath, 'ascii'),
                ca: ca ? fs.readFileSync(capath, 'ascii') : undefined,
            };
            this.httpsPath = {
                ca: capath,
                cert: certpath,
            };
        } else if (key || cert) {
            throw new Error('bad config: both certFilePaths.key and ' +
                'certFilePaths.cert must be defined');
        }
        /**
         * Configure the backends for Authentication, Data and Metadata.
         */
        let auth = 'mem';
        let data = 'file';
        let metadata = 'file';
        let kms = 'file';
        if (process.env.S3BACKEND) {
            const validBackends = ['mem', 'file', 'scality'];
            assert(validBackends.indexOf(process.env.S3BACKEND) > -1,
                'bad environment variable: S3BACKEND environment variable ' +
                'should be one of mem/file/scality'
            );
            auth = process.env.S3BACKEND;
            data = process.env.S3BACKEND;
            metadata = process.env.S3BACKEND;
            kms = process.env.S3BACKEND;
        }
        if (process.env.S3VAULT) {
            auth = process.env.S3VAULT;
        }
        if (auth === 'file' || auth === 'mem') {
            // Auth only checks for 'mem' since mem === file
            auth = 'mem';
            let authfile = `${__dirname}/../conf/authdata.json`;
            if (process.env.S3AUTH_CONFIG) {
                authfile = process.env.S3AUTH_CONFIG;
            }
            const authData = require(authfile);
            if (authDataChecker(authData)) {
                throw new Error('bad config: invalid auth config file.');
            }
            this.authData = authData;
        }
        if (process.env.S3DATA) {
            const validData = ['mem', 'file', 'scality', 'multiple'];
            assert(validData.indexOf(process.env.S3DATA) > -1,
                'bad environment variable: S3DATA environment variable ' +
                'should be one of mem/file/scality/multiple'
            );
            if (process.env.S3DATA === 'multiple') {
                assert(config.locationConstraints !== undefined,
                    'for multiple data backends, locationConstraints ' +
                    'must be set'
                );
            }
            data = process.env.S3DATA;
        }
        if (process.env.S3METADATA) {
            metadata = process.env.S3METADATA;
        }
        if (process.env.S3KMS) {
            kms = process.env.S3KMS;
        }
        this.backends = {
            auth,
            data,
            metadata,
            kms,
        };

        /**
         * Configure the file paths for data and metadata
         * if using the file backend.  If no path provided,
         * uses data and metadata at the root of the S3 project directory
         */
        const dataPath = process.env.S3DATAPATH ?
            process.env.S3DATAPATH : `${__dirname}/../localData`;
        const metadataPath = process.env.S3METADATAPATH ?
            process.env.S3METADATAPATH : `${__dirname}/../localMetadata`;
        this.filePaths = {
            dataPath,
            metadataPath,
        };
        return config;
    }
}

export default new Config();
