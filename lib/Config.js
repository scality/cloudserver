const assert = require('assert');
const { EventEmitter } = require('events');
const fs = require('fs');
const path = require('path');
const url = require('url');

const uuid = require('node-uuid');

const { isValidBucketName } = require('arsenal').s3routes.routesUtils;
const validateAuthConfig = require('arsenal').auth.inMemory.validateAuthConfig;
const { buildAuthDataAccount } = require('./auth/in_memory/builder');
const externalBackends = require('../constants').externalBackends;
const { azureAccountNameRegex, base64Regex } = require('../constants');

// whitelist IP, CIDR for health checks
const defaultHealthChecks = { allowFrom: ['127.0.0.1/8', '::1'] };

const defaultLocalCache = { host: '127.0.0.1', port: 6379 };

function assertCertPaths(key, cert, ca, basePath) {
    const certObj = {};
    certObj.paths = {};
    certObj.certs = {};
    if (key) {
        const keypath = key.startsWith('/') ? key : `${basePath}/${key}`;
        assert.doesNotThrow(() =>
            fs.accessSync(keypath, fs.F_OK | fs.R_OK),
            `File not found or unreachable: ${keypath}`);
        certObj.paths.key = keypath;
        certObj.certs.key = fs.readFileSync(keypath, 'ascii');
    }
    if (cert) {
        const certpath = cert.startsWith('/') ? cert : `${basePath}/${cert}`;
        assert.doesNotThrow(() =>
            fs.accessSync(certpath, fs.F_OK | fs.R_OK),
            `File not found or unreachable: ${certpath}`);
        certObj.paths.cert = certpath;
        certObj.certs.cert = fs.readFileSync(certpath, 'ascii');
    }

    if (ca) {
        const capath = ca.startsWith('/') ? ca : `${basePath}/${ca}`;
        assert.doesNotThrow(() =>
            fs.accessSync(capath, fs.F_OK | fs.R_OK),
            `File not found or unreachable: ${capath}`);
        certObj.paths.ca = capath;
        certObj.certs.ca = fs.readFileSync(capath, 'ascii');
    }
    return certObj;
}

function sproxydAssert(configSproxyd) {
    const sproxydFields = [];
    if (configSproxyd.bootstrap !== undefined) {
        assert(Array.isArray(configSproxyd.bootstrap)
            && configSproxyd.bootstrap
                .every(e => typeof e === 'string'),
            'bad config: sproxyd.bootstrap must be an array of strings');
        assert(configSproxyd.bootstrap.length > 0,
                'bad config: sproxyd bootstrap list is empty');
        sproxydFields.push('bootstrap');
    }
    if (configSproxyd.chordCos !== undefined) {
        assert(typeof configSproxyd.chordCos === 'string',
            'bad config: sproxyd.chordCos must be a string');
        assert(configSproxyd.chordCos.match(/^[0-6]{1}$/),
         'bad config: sproxyd.chordCos must be a digit smaller than 7');
        sproxydFields.push('chordCos');
    }
    if (configSproxyd.path !== undefined) {
        assert(typeof configSproxyd.path === 'string',
            'bad config: sproxyd.path must be a string');
        sproxydFields.push('path');
    }
    return sproxydFields;
}

function restEndpointsAssert(restEndpoints, locationConstraints) {
    assert(typeof restEndpoints === 'object',
        'bad config: restEndpoints must be an object of endpoints');
    assert(Object.keys(restEndpoints).every(
        r => typeof restEndpoints[r] === 'string'),
        'bad config: each endpoint must be a string');
    assert(Object.keys(restEndpoints).every(
        r => typeof locationConstraints[restEndpoints[r]] === 'object'),
        'bad config: rest endpoint target not in locationConstraints');
}

function gcpLocationConstraintAssert(location, locationObj) {
    const {
        gcpEndpoint,
        bucketName,
        mpuBucketName,
    } = locationObj.details;
    const stringFields = [
        gcpEndpoint,
        bucketName,
        mpuBucketName,
    ];
    stringFields.forEach(field => {
        if (field !== undefined) {
            assert(typeof field === 'string',
                `bad config: ${field} must be a string`);
        }
    });
}

function azureLocationConstraintAssert(location, locationObj) {
    const {
        azureStorageEndpoint,
        azureStorageAccountName,
        azureStorageAccessKey,
        azureContainerName,
    } = locationObj.details;
    const storageEndpointFromEnv =
        process.env[`${location}_AZURE_STORAGE_ENDPOINT`];
    const storageAccountNameFromEnv =
        process.env[`${location}_AZURE_STORAGE_ACCOUNT_NAME`];
    const storageAccessKeyFromEnv =
        process.env[`${location}_AZURE_STORAGE_ACCESS_KEY`];
    const locationParams = {
        azureStorageEndpoint: storageEndpointFromEnv || azureStorageEndpoint,
        azureStorageAccountName:
            storageAccountNameFromEnv || azureStorageAccountName,
        azureStorageAccessKey: storageAccessKeyFromEnv || azureStorageAccessKey,
        azureContainerName,
    };
    Object.keys(locationParams).forEach(param => {
        const value = locationParams[param];
        assert.notEqual(value, undefined,
            `bad location constraint: "${location}" ${param} ` +
            'must be set in locationConfig or environment variable');
        assert.strictEqual(typeof value, 'string',
            `bad location constraint: "${location}" ${param} ` +
            `"${value}" must be a string`);
    });
    assert(azureAccountNameRegex.test(locationParams.azureStorageAccountName),
        `bad location constraint: "${location}" azureStorageAccountName ` +
        `"${locationParams.storageAccountName}" is an invalid value`);
    assert(base64Regex.test(locationParams.azureStorageAccessKey),
        `bad location constraint: "${location}" ` +
        'azureStorageAccessKey is not a valid base64 string');
    assert(isValidBucketName(azureContainerName, []),
        `bad location constraint: "${location}" ` +
        'azureContainerName is an invalid container name');
}

function locationConstraintAssert(locationConstraints) {
    const supportedBackends =
          ['mem', 'file', 'scality',
           'mongodb'].concat(Object.keys(externalBackends));
    assert(typeof locationConstraints === 'object',
        'bad config: locationConstraints must be an object');
    Object.keys(locationConstraints).forEach(l => {
        assert(typeof locationConstraints[l] === 'object',
            'bad config: locationConstraints[region] must be an object');
        assert(typeof locationConstraints[l].type === 'string',
            'bad config: locationConstraints[region].type is ' +
            'mandatory and must be a string');
        assert(supportedBackends.indexOf(locationConstraints[l].type) > -1,
            'bad config: locationConstraints[region].type must ' +
            `be one of ${supportedBackends}`);
        assert(typeof locationConstraints[l].legacyAwsBehavior
            === 'boolean',
            'bad config: locationConstraints[region]' +
            '.legacyAwsBehavior is mandatory and must be a boolean');
        if (locationConstraints[l].details.serverSideEncryption !== undefined) {
            assert(typeof locationConstraints[l].details.serverSideEncryption
              === 'boolean',
              'bad config: locationConstraints[region]' +
              '.details.serverSideEncryption must be a boolean');
        }
        assert(typeof locationConstraints[l].details
            === 'object',
            'bad config: locationConstraints[region].details is ' +
            'mandatory and must be an object');
        const details = locationConstraints[l].details;
        const stringFields = [
            'awsEndpoint',
            'bucketName',
            'credentialsProfile',
        ];
        stringFields.forEach(field => {
            if (details[field] !== undefined) {
                assert(typeof details[field] === 'string',
                    `bad config: ${field} must be a string`);
            }
        });
        if (details.bucketMatch !== undefined) {
            assert(typeof details.bucketMatch === 'boolean',
                'bad config: details.bucketMatch must be a boolean');
        }
        if (details.credentials !== undefined) {
            assert(typeof details.credentials === 'object',
                'bad config: details.credentials must be an object');
            assert(typeof details.credentials.accessKey === 'string',
                'bad config: credentials must include accessKey as string');
            assert(typeof details.credentials.secretKey === 'string',
                'bad config: credentials must include secretKey as string');
        }
        if (details.https !== undefined) {
            assert(typeof details.https === 'boolean', 'bad config: ' +
                'locationConstraints[region].details https must be a boolean');
        } else {
            // eslint-disable-next-line no-param-reassign
            locationConstraints[l].details.https = true;
        }
        if (locationConstraints[l].type === 'azure') {
            azureLocationConstraintAssert(l, locationConstraints[l]);
        }
        if (locationConstraints[l].type === 'gcp') {
            gcpLocationConstraintAssert(l, locationConstraints[l]);
        }
    });
    assert(Object.keys(locationConstraints)
        .includes('us-east-1'), 'bad locationConfig: must ' +
        'include us-east-1 as a locationConstraint');
}

function cosParse(chordCos) {
    // Cos number should only be first digit of config value
    return Number.parseInt(chordCos, 10);
}
/**
 * Reads from a config file and returns the content as a config object
 */
class Config extends EventEmitter {
    constructor() {
        super();
        /*
         * By default, the config file is "config.json" at the root.
         * It can be overridden using the S3_CONFIG_FILE environment var.
         * By default, the location config file is "locationConfig.json" at
         * the root.
         * It can be overridden using the S3_LOCATION_FILE environment var.
         */
        this._basePath = path.join(__dirname, '..');
        this.configPath = path.join(__dirname, '../config.json');
        if (process.env.S3_CONFIG_FILE !== undefined) {
            this.configPath = process.env.S3_CONFIG_FILE;
        }
        this.locationConfigPath = path.join(__dirname,
          '../locationConfig.json');
        if (process.env.CI === 'true' && !process.env.S3_END_TO_END) {
            this.locationConfigPath = path.join(__dirname,
                '../tests/locationConfig/locationConfigTests.json');
        }
        if (process.env.S3_LOCATION_FILE !== undefined) {
            this.locationConfigPath = process.env.S3_LOCATION_FILE;
        }

        // Read config automatically
        this._getLocationConfig();
        this._getConfig();
        this._configureBackends();
    }

    _getLocationConfig() {
        let locationConfig;
        try {
            const data = fs.readFileSync(this.locationConfigPath,
            { encoding: 'utf-8' });
            locationConfig = JSON.parse(data);
        } catch (err) {
            throw new Error(`could not parse location config file:
            ${err.message}`);
        }

        this.locationConstraints = {};
        locationConstraintAssert(locationConfig);
        this.locationConstraints = locationConfig;
        Object.keys(locationConfig).forEach(l => {
            const details = this.locationConstraints[l].details;
            if (locationConfig[l].details.connector !== undefined) {
                assert(typeof locationConfig[l].details.connector ===
                'object', 'bad config: connector must be an object');
                if (locationConfig[l].details.connector.sproxyd !==
                  undefined) {
                    details.connector.sproxyd =
                        locationConfig[l].details.connector.sproxyd;
                    const fields = sproxydAssert(
                        locationConfig[l].details.connector.sproxyd);
                    if (fields.indexOf('bootstrap') > -1) {
                        details.connector.sproxyd.bootstrap =
                        locationConfig[l].details.connector.sproxyd.bootstrap;
                        assert(Array.isArray(
                            details.connector.sproxyd.bootstrap) &&
                            details.connector.sproxyd.bootstrap.every(e =>
                                typeof e === 'string'),
                                'assignment error: sproxyd.bootstrap must be ' +
                                'an array of strings');
                    }
                    if (fields.indexOf('chordCos') > -1) {
                        details.connector.sproxyd.chordCos =
                            cosParse(locationConfig[l].details.connector.
                                sproxyd.chordCos);
                        assert(typeof details.connector.sproxyd.chordCos ===
                            'number', 'assignment error: chordCos must be a ' +
                            'number');
                    }
                    if (fields.indexOf('path') > -1) {
                        details.connector.sproxyd.chordCos =
                            locationConfig[l].details.connector.sproxyd.path;
                        assert(typeof details.connector.sproxyd.chordCos ===
                            'string', 'assignment error: sproxyd path must ' +
                            'be a string');
                    }
                }
            }
        });
    }

    _getConfig() {
        let config;
        try {
            const data = fs.readFileSync(this.configPath,
              { encoding: 'utf-8' });
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
                assert(parseInt(port, 10),
                    'bad config: listenOn port must be a positive integer');
                this.listenOn.push({ ip: ipAddress, port });
            });
        }

        if (config.replicationGroupId) {
            assert(typeof config.replicationGroupId === 'string',
                'bad config: replicationGroupId must be a string');
            this.replicationGroupId = config.replicationGroupId;
        } else {
            this.replicationGroupId = 'RG001';
        }

        this.replicationEndpoints = [];
        if (config.replicationEndpoints) {
            const { replicationEndpoints } = config;
            assert(replicationEndpoints instanceof Array, 'bad config: ' +
                '`replicationEndpoints` property must be an array');
            if (replicationEndpoints.length > 1) {
                const hasDefault = replicationEndpoints.some(
                    replicationEndpoint => replicationEndpoint.default);
                assert(hasDefault, 'bad config: `replicationEndpoints` must ' +
                    'contain a default endpoint');
            }
            replicationEndpoints.forEach(replicationEndpoint => {
                assert.strictEqual(typeof replicationEndpoint, 'object',
                    'bad config: `replicationEndpoints` property must be an ' +
                    'array of objects');
                const { site, servers, type } = replicationEndpoint;
                assert.notStrictEqual(site, undefined, 'bad config: each ' +
                    'object of `replicationEndpoints` array must have a ' +
                    '`site` property');
                assert.strictEqual(typeof site, 'string', 'bad config: ' +
                    '`site` property of object in `replicationEndpoints` ' +
                    'must be a string');
                assert.notStrictEqual(site, '', 'bad config: `site` property ' +
                    "of object in `replicationEndpoints` must not be ''");
                if (type !== undefined) {
                    assert(externalBackends[type], 'bad config: `type` ' +
                        'property of `replicationEndpoints` object must be ' +
                        'a valid external backend (one of: "' +
                        `${Object.keys(externalBackends).join('", "')}")`);
                } else {
                    assert.notStrictEqual(servers, undefined, 'bad config: ' +
                        'each object of `replicationEndpoints` array that is ' +
                        'not an external backend must have `servers` property');
                    assert(servers instanceof Array, 'bad config: ' +
                        '`servers` property of object in ' +
                        '`replicationEndpoints` must be an array');
                    servers.forEach(item => {
                        assert(typeof item === 'string' && item !== '',
                        'bad config: each item of ' +
                        '`replicationEndpoints:servers` must be a non-empty ' +
                        'string');
                    });
                }
            });
            this.replicationEndpoints = replicationEndpoints;
        }

        // legacy
        if (config.regions !== undefined) {
            throw new Error('bad config: regions key is deprecated. ' +
                'Please use restEndpoints and locationConfig');
        }

        if (config.restEndpoints !== undefined) {
            this.restEndpoints = {};
            restEndpointsAssert(config.restEndpoints, this.locationConstraints);
            this.restEndpoints = config.restEndpoints;
        }

        if (!config.restEndpoints) {
            throw new Error('bad config: config must include restEndpoints');
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

        if (config.usEastBehavior !== undefined) {
            throw new Error('bad config: usEastBehavior key is deprecated. ' +
                'Please use restEndpoints and locationConfig');
        }
        // legacy
        if (config.sproxyd !== undefined) {
            throw new Error('bad config: sproxyd key is deprecated. ' +
                'Please use restEndpoints and locationConfig');
        }

        this.cdmi = {};
        if (config.cdmi !== undefined) {
            if (config.cdmi.host !== undefined) {
                assert.strictEqual(typeof config.cdmi.host, 'string',
                                   'bad config: cdmi host must be a string');
                this.cdmi.host = config.cdmi.host;
            }
            if (config.cdmi.port !== undefined) {
                assert(Number.isInteger(config.cdmi.port)
                       && config.cdmi.port > 0,
                       'bad config: cdmi port must be a positive integer');
                this.cdmi.port = config.cdmi.port;
            }
            if (config.cdmi.path !== undefined) {
                assert(typeof config.cdmi.path === 'string',
                       'bad config: cdmi.path must be a string');
                assert(config.cdmi.path.length > 0,
                       'bad config: cdmi.path is empty');
                assert(config.cdmi.path.charAt(0) === '/',
                       'bad config: cdmi.path should start with a "/"');
                this.cdmi.path = config.cdmi.path;
            }
            if (config.cdmi.readonly !== undefined) {
                assert(typeof config.cdmi.readonly === 'boolean',
                       'bad config: cdmi.readonly must be a boolean');
                this.cdmi.readonly = config.cdmi.readonly;
            } else {
                this.cdmi.readonly = true;
            }
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

        if (config.dataClient) {
            this.dataClient = {};
            assert.strictEqual(typeof config.dataClient.host, 'string',
                               'bad config: data client host must be ' +
                               'a string');
            this.dataClient.host = config.dataClient.host;

            assert(Number.isInteger(config.dataClient.port)
                   && config.dataClient.port > 0,
                   'bad config: dataClient port must be a positive ' +
                   'integer');
            this.dataClient.port = config.dataClient.port;
        }

        if (config.metadataClient) {
            this.metadataClient = {};
            assert.strictEqual(
                typeof config.metadataClient.host, 'string',
                'bad config: metadata client host must be a string');
            this.metadataClient.host = config.metadataClient.host;

            assert(Number.isInteger(config.metadataClient.port)
                   && config.metadataClient.port > 0,
                   'bad config: metadata client port must be a ' +
                   'positive integer');
            this.metadataClient.port = config.metadataClient.port;
        }

        if (config.dataDaemon) {
            this.dataDaemon = {};
            assert.strictEqual(
                typeof config.dataDaemon.bindAddress, 'string',
                'bad config: data daemon bind address must be a string');
            this.dataDaemon.bindAddress = config.dataDaemon.bindAddress;

            assert(Number.isInteger(config.dataDaemon.port)
                   && config.dataDaemon.port > 0,
                   'bad config: data daemon port must be a positive ' +
                   'integer');
            this.dataDaemon.port = config.dataDaemon.port;

            /**
             * Configure the file paths for data if using the file
             * backend. If no path provided, uses data at the root of
             * the S3 project directory.
             */
            this.dataDaemon.dataPath =
                process.env.S3DATAPATH ?
                process.env.S3DATAPATH : `${__dirname}/../localData`;
        }

        if (config.metadataDaemon) {
            this.metadataDaemon = {};
            assert.strictEqual(
                typeof config.metadataDaemon.bindAddress, 'string',
                'bad config: metadata daemon bind address must be a string');
            this.metadataDaemon.bindAddress =
                config.metadataDaemon.bindAddress;

            assert(Number.isInteger(config.metadataDaemon.port)
                   && config.metadataDaemon.port > 0,
                   'bad config: metadata daemon port must be a ' +
                   'positive integer');
            this.metadataDaemon.port = config.metadataDaemon.port;

            /**
             * Configure the file path for metadata if using the file
             * backend. If no path provided, uses data and metadata at
             * the root of the S3 project directory.
             */
            this.metadataDaemon.metadataPath =
                process.env.S3METADATAPATH ?
                process.env.S3METADATAPATH : `${__dirname}/../localMetadata`;

            this.metadataDaemon.restEnabled =
                config.metadataDaemon.restEnabled;
            this.metadataDaemon.restPort = config.metadataDaemon.restPort;
        }

        this.recordLog = { enabled: false };
        if (config.recordLog) {
            this.recordLog.enabled = Boolean(config.recordLog.enabled);
            this.recordLog.recordLogName = config.recordLog.recordLogName;
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
            if (config.localCache.password !== undefined) {
                assert(this._verifyRedisPassword(config.localCache.password),
                    'config: invalid password for localCache. password must' +
                    ' be a string');
            }
            this.localCache = {
                host: config.localCache.host,
                port: config.localCache.port,
                password: config.localCache.password,
            };
        }

        if (config.mongodb) {
            this.mongodb = config.mongodb;
        } else {
            this.mongodb = {};
        }

        if (config.utapi) {
            this.utapi = { component: 's3' };
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
            // Utapi uses the same localCache config defined for S3 to avoid
            // config duplication.
            assert(config.localCache, 'missing required property of utapi ' +
                'configuration: localCache');
            this.utapi.localCache = config.localCache;
            assert(config.utapi.redis, 'missing required property of utapi ' +
                'configuration: redis');
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
            if (config.utapi.redis.password !== undefined) {
                assert(
                    this._verifyRedisPassword(config.utapi.redis.password),
                    'config: invalid password for utapi redis. password' +
                    ' must be a string');
                this.utapi.redis.password = config.utapi.redis.password;
            }
            if (config.utapi.metrics) {
                this.utapi.metrics = config.utapi.metrics;
            }
            if (config.utapi.component) {
                this.utapi.component = config.utapi.component;
            }
            // (optional) The value of the replay schedule should be cron-style
            // scheduling. For example, every five minutes: '*/5 * * * *'.
            if (config.utapi.replaySchedule) {
                assert(typeof config.utapi.replaySchedule === 'string', 'bad' +
                    'config: utapi.replaySchedule must be a string');
                this.utapi.replaySchedule = config.utapi.replaySchedule;
            }
            // (optional) The number of elements processed by each call to the
            // Redis local cache during a replay. For example, 50.
            if (config.utapi.batchSize) {
                assert(typeof config.utapi.batchSize === 'number', 'bad' +
                    'config: utapi.batchSize must be a number');
                assert(config.utapi.batchSize > 0, 'bad config:' +
                    'utapi.batchSize must be a number greater than 0');
                this.utapi.batchSize = config.utapi.batchSize;
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
        let certObj = undefined;
        if (key && cert) {
            certObj = assertCertPaths(key, cert, ca, this._basePath);
        } else if (key || cert) {
            throw new Error('bad config: both certFilePaths.key and ' +
                'certFilePaths.cert must be defined');
        }
        if (certObj) {
            if (Object.keys(certObj.certs).length > 0) {
                this.https = certObj.certs;
            }
            if (Object.key(certObj.paths).length > 0) {
                this.httpsPath = certObj.certs;
            }
        }

        this.outboundProxy = {};
        const envProxy = process.env.HTTP_PROXY || process.env.HTTPS_PROXY
            || process.env.http_proxy || process.env.https_proxy;
        const p = config.outboundProxy;
        const proxyUrl = envProxy || (p ? p.url : '');
        if (proxyUrl) {
            assert(typeof proxyUrl === 'string',
                'bad proxy config: url must be a string');
            const { protocol, hostname, port, auth } = url.parse(proxyUrl);
            assert(protocol === 'http:' || protocol === 'https:',
                'bad proxy config: protocol must be http or https');
            assert(typeof hostname === 'string' && hostname !== '',
                'bad proxy config: hostname must be a non-empty string');
            if (port) {
                const portInt = Number.parseInt(port, 10);
                assert(!Number.isNaN(portInt) && portInt > 0,
                    'bad proxy config: port must be a number greater than 0');
            }
            if (auth) {
                assert(typeof auth === 'string',
                    'bad proxy config: auth must be string');
                const authArray = auth.split(':');
                assert(authArray.length === 2 && authArray[0].length > 0
                    && authArray[1].length > 0, 'bad proxy config: ' +
                    'auth must be of format username:password');
            }
            this.outboundProxy.url = proxyUrl;
            this.outboundProxy.certFilePaths = {};
            if (p) {
                assert(typeof p === 'object',
                    'bad config: "proxy" should be an object');
                const { key, cert, caBundle } = p;
                if (key) {
                    assert(typeof key === 'string',
                        'bad config: proxy.key should be a string');
                }
                if (cert) {
                    assert(typeof cert === 'string',
                        'bad config: proxy.cert should be a string');
                }
                if (caBundle) {
                    assert(typeof caBundle === 'string',
                        'bad config: proxy.caBundle should be a string');
                }
                const certObj =
                    assertCertPaths(key, cert, caBundle, this._basePath);
                this.outboundProxy.certs = certObj.certs;
            }
        }

        // Ephemeral token to protect the reporting endpoint:
        // try inherited from parent first, then hardcoded in conf file,
        // then create a fresh one as last resort.
        this.reportToken =
            process.env.REPORT_TOKEN ||
            config.reportToken ||
            uuid.v4().toString();
    }

    _configureBackends() {
        /**
         * Configure the backends for Authentication, Data and Metadata.
         */
        let auth = 'mem';
        let data = 'file';
        let metadata = 'file';
        let kms = 'file';
        if (process.env.S3BACKEND) {
            const validBackends = ['mem', 'file', 'scality', 'cdmi'];
            assert(validBackends.indexOf(process.env.S3BACKEND) > -1,
                'bad environment variable: S3BACKEND environment variable ' +
                'should be one of mem/file/scality/cdmi'
            );
            auth = process.env.S3BACKEND;
            data = process.env.S3BACKEND;
            metadata = process.env.S3BACKEND;
            kms = process.env.S3BACKEND;
        }
        if (process.env.S3VAULT) {
            auth = process.env.S3VAULT;
        }
        if (auth === 'file' || auth === 'mem' || auth === 'cdmi') {
            // Auth only checks for 'mem' since mem === file
            auth = 'mem';
            let authfile = `${__dirname}/../conf/authdata.json`;
            if (process.env.S3AUTH_CONFIG) {
                authfile = process.env.S3AUTH_CONFIG;
            }
            let authData;
            if (process.env.SCALITY_ACCESS_KEY_ID &&
            process.env.SCALITY_SECRET_ACCESS_KEY) {
                authData = buildAuthDataAccount(
                  process.env.SCALITY_ACCESS_KEY_ID,
                  process.env.SCALITY_SECRET_ACCESS_KEY);
            } else {
                authData = require(authfile);
            }
            if (validateAuthConfig(authData)) {
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
            data = process.env.S3DATA;
        }
        if (data === 'scality' || data === 'multiple') {
            data = 'multiple';
        }
        assert(this.locationConstraints !== undefined &&
            this.restEndpoints !== undefined,
            'bad config: locationConstraints and restEndpoints must be set'
        );

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
    }

    _verifyRedisPassword(password) {
        return typeof password === 'string';
    }

    setAuthDataAccounts(accounts) {
        this.authData.accounts = accounts;
        this.emit('authdata-update');
    }

    getAwsBucketName(locationConstraint) {
        return this.locationConstraints[locationConstraint].details.bucketName;
    }

    getGcpBucketNames(locationConstraint) {
        const {
            bucketName,
            mpuBucketName,
        } = this.locationConstraints[locationConstraint].details;
        return { bucketName, mpuBucketName };
    }

    getLocationConstraintType(locationConstraint) {
        const dataStoreName = this.locationConstraints[locationConstraint];
        return dataStoreName && dataStoreName.type;
    }

    setRestEndpoints(restEndpoints) {
        restEndpointsAssert(restEndpoints, this.locationConstraints);
        this.restEndpoints = restEndpoints;
        this.emit('rest-endpoints-update');
    }

    setLocationConstraints(locationConstraints) {
        restEndpointsAssert(this.restEndpoints, locationConstraints);
        this.locationConstraints = locationConstraints;
        this.emit('location-constraints-update');
    }

    getAzureEndpoint(locationConstraint) {
        let azureStorageEndpoint =
        process.env[`${locationConstraint}_AZURE_STORAGE_ENDPOINT`] ||
        this.locationConstraints[locationConstraint]
            .details.azureStorageEndpoint;
        if (!azureStorageEndpoint.endsWith('/')) {
            // append the trailing slash
            azureStorageEndpoint = `${azureStorageEndpoint}/`;
        }
        return azureStorageEndpoint;
    }

    getAzureStorageAccountName(locationConstraint) {
        const { azureStorageAccountName } =
            this.locationConstraints[locationConstraint].details;
        const storageAccountNameFromEnv =
            process.env[`${locationConstraint}_AZURE_STORAGE_ACCOUNT_NAME`];
        return storageAccountNameFromEnv || azureStorageAccountName;
    }

    getAzureStorageCredentials(locationConstraint) {
        const { azureStorageAccessKey } =
            this.locationConstraints[locationConstraint].details;
        const storageAccessKeyFromEnv =
            process.env[`${locationConstraint}_AZURE_STORAGE_ACCESS_KEY`];
        return {
            storageAccountName:
                this.getAzureStorageAccountName(locationConstraint),
            storageAccessKey: storageAccessKeyFromEnv || azureStorageAccessKey,
        };
    }

    isSameAzureAccount(locationConstraintSrc, locationConstraintDest) {
        if (!locationConstraintDest) {
            return true;
        }
        const azureSrcAccount =
            this.getAzureStorageAccountName(locationConstraintSrc);
        const azureDestAccount =
            this.getAzureStorageAccountName(locationConstraintDest);
        return azureSrcAccount === azureDestAccount;
    }

    isAWSServerSideEncrytion(locationConstraint) {
        return this.locationConstraints[locationConstraint].details
        .serverSideEncryption === true;
    }
}

module.exports = {
    sproxydAssert,
    locationConstraintAssert,
    cosParse,
    ConfigObject: Config,
    config: new Config(),
};
