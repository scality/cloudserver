const assert = require('assert');
const UUID = require('node-uuid');

const config = require('../../../../Config').config;
const { errors, s3middleware, s3routes } = require('arsenal');
const escapeForXml = s3middleware.escapeForXml;
const { isValidBucketName } = s3routes.routesUtils;

const MAX_RULES = 1000;
const RULE_ID_LIMIT = 255;
const validStorageClasses = [
    undefined,
    'STANDARD',
    'STANDARD_IA',
    'REDUCED_REDUNDANCY',
];

/**
    Example XML request:

    <ReplicationConfiguration>
        <Role>IAM-role-ARN</Role>
        <Rule>
            <ID>Rule-1</ID>
            <Status>rule-status</Status>
            <Prefix>key-prefix</Prefix>
            <Destination>
                <Bucket>arn:aws:s3:::bucket-name</Bucket>
                <StorageClass>
                    optional-destination-storage-class-override
                </StorageClass>
            </Destination>
        </Rule>
        <Rule>
            <ID>Rule-2</ID>
            ...
        </Rule>
        ...
    </ReplicationConfiguration>
*/

class ReplicationConfiguration {
    /**
     * Create a ReplicationConfiguration instance
     * @param {string} xml - The parsed XML
     * @param {object} log - Werelogs logger
     * @return {object} - ReplicationConfiguration instance
     */
    constructor(xml, log) {
        this._parsedXML = xml;
        this._log = log;
        this._configPrefixes = [];
        this._configIDs = [];
        // The bucket metadata model of replication config. Note there is a
        // single `destination` property because we can replicate to only one
        // other bucket. Thus each rule is simplified to these properties.
        this._role = null;
        this._destination = null;
        this._rules = null;
    }

    /**
     * Get the role of the bucket replication configuration
     * @return {string|null} - The role if defined, otherwise `null`
     */
    getRole() {
        return this._role;
    }

    /**
     * The bucket to replicate data to
     * @return {string|null} - The bucket if defined, otherwise `null`
     */
    getDestination() {
        return this._destination;
    }

    /**
     * The rules for replication configuration
     * @return {string|null} - The rules if defined, otherwise `null`
     */
    getRules() {
        return this._rules;
    }

    /**
     * Get the replication configuration
     * @return {object} - The replication configuration
     */
    getReplicationConfiguration() {
        return {
            role: this.getRole(),
            destination: this.getDestination(),
            rules: this.getRules(),
        };
    }

    /**
     * Build the rule object from the parsed XML of the given rule
     * @param {object} rule - The rule object from this._parsedXML
     * @return {object} - The rule object to push into the `Rules` array
     */
    _buildRuleObject(rule) {
        const obj = {
            prefix: rule.Prefix[0],
            enabled: rule.Status[0] === 'Enabled',
        };
        // ID is an optional property, but create one if not provided or is ''.
        // We generate a 48-character alphanumeric, unique ID for the rule.
        obj.id = rule.ID && rule.ID[0] !== '' ? rule.ID[0] :
            Buffer.from(UUID.v4()).toString('base64');
        // StorageClass is an optional property.
        if (rule.Destination[0].StorageClass) {
            obj.storageClass = rule.Destination[0].StorageClass[0];
        }
        return obj;
    }

    /**
     * Check if the Role field of the replication configuration is valid
     * @param {string} ARN - The Role field value provided in the configuration
     * @return {boolean} `true` if a valid role ARN, `false` otherwise
     */
    _isValidRoleARN(ARN) {
        // AWS accepts a range of values for the Role field. Though this does
        // not encompass all constraints imposed by AWS, we have opted to
        // enforce the following.
        const arr = ARN.split(':');
        const isValidRoleARN =
            arr[0] === 'arn' &&
            arr[1] === 'aws' &&
            arr[2] === 'iam' &&
            arr[3] === '' &&
            (arr[4] === '*' || arr[4].length > 1) &&
            arr[5].startsWith('role');
        return isValidRoleARN;
    }

    /**
     * Check that the `Role` property of the configuration is valid
     * @return {undefined}
     */
    _parseRole() {
        const parsedRole = this._parsedXML.ReplicationConfiguration.Role;
        if (!parsedRole) {
            return errors.MalformedXML;
        }
        const role = parsedRole[0];
        const rolesArr = role.split(',');
        if (rolesArr.length !== 2) {
            return errors.InvalidArgument.customizeDescription(
                'Invalid Role specified in replication configuration: ' +
                'Role must be a comma-separated list of two IAM roles');
        }
        const invalidRole = rolesArr.find(r => !this._isValidRoleARN(r));
        if (invalidRole !== undefined) {
            return errors.InvalidArgument.customizeDescription(
                'Invalid Role specified in replication configuration: ' +
                `'${invalidRole}'`);
        }
        this._role = role;
        return undefined;
    }

    /**
     * Check that the `Rules` property array is valid
     * @return {undefined}
     */
    _parseRules() {
        // Note that the XML uses 'Rule' while the config object uses 'Rules'.
        const { Rule } = this._parsedXML.ReplicationConfiguration;
        if (!Rule || Rule.length < 1) {
            return errors.MalformedXML;
        }
        if (Rule.length > MAX_RULES) {
            return errors.InvalidRequest.customizeDescription(
                'Number of defined replication rules cannot exceed 1000');
        }
        const err = this._parseEachRule(Rule);
        if (err) {
            return err;
        }
        return undefined;
    }

    /**
     * Check that each rule in the `Rules` property array is valid
     * @param {array} rules - The rule array from this._parsedXML
     * @return {undefined}
     */
    _parseEachRule(rules) {
        const rulesArr = [];
        for (let i = 0; i < rules.length; i++) {
            const err =
                this._parseStatus(rules[i]) || this._parsePrefix(rules[i]) ||
                this._parseID(rules[i]) || this._parseDestination(rules[i]);
            if (err) {
                return err;
            }
            rulesArr.push(this._buildRuleObject(rules[i]));
        }
        this._rules = rulesArr;
        return undefined;
    }

    /**
     * Check that the `Status` property is valid
     * @param {object} rule - The rule object from this._parsedXML
     * @return {undefined}
     */
    _parseStatus(rule) {
        const status = rule.Status && rule.Status[0];
        if (!status || !['Enabled', 'Disabled'].includes(status)) {
            return errors.MalformedXML;
        }
        return undefined;
    }

    /**
     * Check that the `Prefix` property is valid
     * @param {object} rule - The rule object from this._parsedXML
     * @return {undefined}
     */
    _parsePrefix(rule) {
        const prefix = rule.Prefix && rule.Prefix[0];
        // An empty string prefix should be allowed.
        if (!prefix && prefix !== '') {
            return errors.MalformedXML;
        }
        if (prefix.length > 1024) {
            return errors.InvalidArgument.customizeDescription('Rule prefix ' +
                'cannot be longer than maximum allowed key length of 1024');
        }
        // Each Prefix in a list of rules must not overlap. For example, two
        // prefixes 'TaxDocs' and 'TaxDocs/2015' are overlapping. An empty
        // string prefix is expected to overlap with any other prefix.
        for (let i = 0; i < this._configPrefixes.length; i++) {
            const used = this._configPrefixes[i];
            if (prefix.startsWith(used) || used.startsWith(prefix)) {
                return errors.InvalidRequest.customizeDescription('Found ' +
                    `overlapping prefixes '${used}' and '${prefix}'`);
            }
        }
        this._configPrefixes.push(prefix);
        return undefined;
    }

    /**
     * Check that the `ID` property is valid
     * @param {object} rule - The rule object from this._parsedXML
     * @return {undefined}
     */
    _parseID(rule) {
        const id = rule.ID && rule.ID[0];
        if (id && id.length > RULE_ID_LIMIT) {
            return errors.InvalidArgument
                .customizeDescription('Rule Id cannot be greater than 255');
        }
        // Each ID in a list of rules must be unique.
        if (this._configIDs.includes(id)) {
            return errors.InvalidRequest.customizeDescription(
                'Rule Id must be unique');
        }
        this._configIDs.push(id);
        return undefined;
    }

    /**
     * Check that the `StorageClass` is a valid class
     * @param {string} storageClass - The storage class to validate
     * @return {boolean} `true` if valid, otherwise `false`
     */
    static _isValidStorageClass(storageClass) {
        const replicationEndpoints = config.replicationEndpoints
            .map(endpoint => endpoint.name);
        return replicationEndpoints.includes(storageClass) ||
            validStorageClasses.includes(storageClass);
    }

    /**
     * Check that the `StorageClass` property is valid
     * @param {object} destination - The destination object from this._parsedXML
     * @return {undefined}
     */
    _parseStorageClass(destination) {
        const storageClass = destination.StorageClass &&
            destination.StorageClass[0];
        if (!ReplicationConfiguration._isValidStorageClass(storageClass)) {
            return errors.MalformedXML;
        }
        return undefined;
    }

    /**
     * Check that the `Bucket` property is valid
     * @param {object} destination - The destination object from this._parsedXML
     * @return {undefined}
     */
    _parseBucket(destination) {
        const parsedBucketARN = destination.Bucket;
        if (!parsedBucketARN) {
            return errors.MalformedXML;
        }
        const bucketARN = parsedBucketARN[0];
        if (!bucketARN) {
            return errors.InvalidArgument.customizeDescription(
                'Destination bucket cannot be null or empty');
        }
        const arr = bucketARN.split(':');
        const isValidARN =
            arr[0] === 'arn' &&
            arr[1] === 'aws' &&
            arr[2] === 's3' &&
            arr[3] === '' &&
            arr[4] === '';
        if (!isValidARN) {
            return errors.InvalidArgument
                .customizeDescription('Invalid bucket ARN');
        }
        if (!isValidBucketName(arr[5], [])) {
            return errors.InvalidArgument
                .customizeDescription('The specified bucket is not valid');
        }
        // We can replicate objects only to one destination bucket.
        if (this._destination && this._destination !== bucketARN) {
            return errors.InvalidRequest.customizeDescription(
                'The destination bucket must be same for all rules');
        }
        this._destination = bucketARN;
        return undefined;
    }

    /**
     * Check that the `destination` property is valid
     * @param {object} rule - The rule object from this._parsedXML
     * @return {undefined}
     */
    _parseDestination(rule) {
        const dest = rule.Destination && rule.Destination[0];
        if (!dest) {
            return errors.MalformedXML;
        }
        const err = this._parseBucket(dest) || this._parseStorageClass(dest);
        if (err) {
            return err;
        }
        return undefined;
    }

    /**
     * Check that the request configuration is valid
     * @return {undefined}
     */
    parseConfiguration() {
        const err = this._parseRole() || this._parseRules();
        if (err) {
            return err;
        }
        return undefined;
    }

    /**
     * Get the XML representation of the configuration object
     * @param {object} config - The bucket replication configuration
     * @return {string} - The XML representation of the configuration
     */
    static getConfigXML(config) {
        const { role, destination, rules } = config;
        const Role = `<Role>${escapeForXml(role)}</Role>`;
        const Bucket = `<Bucket>${escapeForXml(destination)}</Bucket>`;
        const rulesXML = rules.map(rule => {
            const { prefix, enabled, storageClass, id } = rule;
            const Prefix = prefix === '' ? '<Prefix/>' :
                `<Prefix>${escapeForXml(prefix)}</Prefix>`;
            const Status =
                `<Status>${enabled ? 'Enabled' : 'Disabled'}</Status>`;
            const StorageClass = storageClass ?
                `<StorageClass>${storageClass}</StorageClass>` : '';
            const Destination =
                `<Destination>${Bucket}${StorageClass}</Destination>`;
            // If the ID property was omitted in the configuration object, we
            // create an ID for the rule. Hence it is always defined.
            const ID = `<ID>${escapeForXml(id)}</ID>`;
            return `<Rule>${ID}${Prefix}${Status}${Destination}</Rule>`;
        }).join('');
        return '<?xml version="1.0" encoding="UTF-8"?>' +
            '<ReplicationConfiguration ' +
                'xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
                `${rulesXML}${Role}` +
            '</ReplicationConfiguration>';
    }

    /**
     * Validate the bucket metadata replication configuration structure and
     * value types
     * @param {object} config - The replication configuration to validate
     * @return {undefined}
     */
    static validateConfig(config) {
        assert.strictEqual(typeof config, 'object');
        const { role, rules, destination } = config;
        assert.strictEqual(typeof role, 'string');
        assert.strictEqual(typeof destination, 'string');
        assert.strictEqual(Array.isArray(rules), true);
        rules.forEach(rule => {
            assert.strictEqual(typeof rule, 'object');
            const { prefix, enabled, id, storageClass } = rule;
            assert.strictEqual(typeof prefix, 'string');
            assert.strictEqual(typeof enabled, 'boolean');
            assert(id === undefined || typeof id === 'string');
            assert(this._isValidStorageClass(storageClass) === true);
        });
    }
}

module.exports = ReplicationConfiguration;
