const { errorInstances } = require('@scality/arsenal');
const { isLocalAddress, isValidHostPort, isValidKafkaTopic } = require('./checks');

/**
 * The checker of a destination body, as Create and Update receive it. It
 * takes the body that parseXML gives. It returns either { error } or the
 * fields of the record. The error is an InvalidDestination that names the
 * field at fault. The fields are Type, Connection and Auth, with plain
 * values: a list of host:port strings for Brokers, a number for
 * RequiredAcks and strings elsewhere. The Connection rules depend on the
 * Type. The Auth rules are shared. An optional Auth block (Sasl, Tls) that
 * the body omits is absent from the result. No error description
 * holds a value of the body, because the body carries secrets.
 */

const types = ['kafka'];
const requiredAcksValues = ['-1', '0', '1'];
const compressionTypes = ['none', 'gzip', 'snappy', 'lz4', 'zstd'];
const authTypes = ['none', 'sasl_plaintext', 'sasl_ssl', 'mtls'];
const saslAuthTypes = ['sasl_plaintext', 'sasl_ssl'];
// GSSAPI needs a keytab and a principal, which a destination cannot hold.
const saslMechanisms = ['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512'];
const defaultRequiredAcks = -1;
const defaultCompressionType = 'none';
const maxBrokers = 10;

// The Auth types that take a Tls block. For each type: whether it needs the
// block, the fields that the block takes, and the fields that it requires.
// sasl_ssl takes an optional block that carries the CA certificate. mtls
// needs a block with the client certificate and key.
const tlsFields = {
    // eslint-disable-next-line camelcase
    sasl_ssl: { blockRequired: false, allowed: ['CaCertificate'], required: ['CaCertificate'] },
    mtls: {
        blockRequired: true,
        allowed: ['CaCertificate', 'ClientCertificate', 'ClientKey', 'ClientKeyPassword'],
        required: ['ClientCertificate', 'ClientKey'],
    },
};

class InvalidField extends Error {}

function invalid(description) {
    throw new InvalidField(description);
}

/**
 * Read the elements of one block of the parsed body. xml2js gives each
 * element as an array, an empty block as an empty string, and the attributes
 * of an element, such as xmlns, under '$'. This function ignores '$'.
 * @param {object|string} node - the parsed block
 * @param {string} path - the name of the block in error descriptions
 * @param {string[]} allowed - the element names that the block takes
 * @return {object} - each element of the block, by name
 */
function readBlock(node, path, allowed) {
    if (node === '') {
        return {};
    }
    if (node === null || typeof node !== 'object' || Array.isArray(node)) {
        return invalid(`${path} must be a block of elements.`);
    }
    const block = {};
    Object.keys(node).forEach(name => {
        if (name === '$') {
            return;
        }
        const fieldPath = `${path}.${name}`;
        if (!allowed.includes(name)) {
            invalid(`${fieldPath} is not a known field.`);
        }
        if (!Array.isArray(node[name]) || node[name].length !== 1) {
            invalid(`${fieldPath} must appear once.`);
        }
        block[name] = node[name][0];
    });
    return block;
}

function readString(block, path, name, required) {
    const fieldPath = `${path}.${name}`;
    const value = block[name];
    if (value === undefined) {
        return required ? invalid(`${fieldPath} is required.`) : undefined;
    }
    if (typeof value !== 'string' || value === '') {
        return invalid(`${fieldPath} must be a non-empty string.`);
    }
    return value;
}

function readEnum(block, path, name, values, required) {
    const value = readString(block, path, name, required);
    if (value !== undefined && !values.includes(value)) {
        invalid(`${path}.${name} must be one of ${values.join(', ')}.`);
    }
    return value;
}

function readBrokers(block, path) {
    const fieldPath = `${path}.Brokers`;
    const node = block.Brokers;
    if (node === undefined) {
        return invalid(`${fieldPath} is required.`);
    }
    const count = `${fieldPath} must hold 1 to ${maxBrokers} Broker elements.`;
    if (node === '') {
        return invalid(count);
    }
    if (node === null || typeof node !== 'object' || Array.isArray(node)) {
        return invalid(`${fieldPath} must be a block of Broker elements.`);
    }
    Object.keys(node).forEach(name => {
        if (name !== '$' && name !== 'Broker') {
            invalid(`${fieldPath}.${name} is not a known field.`);
        }
    });
    const brokers = node.Broker;
    if (!Array.isArray(brokers) || brokers.length < 1 || brokers.length > maxBrokers) {
        return invalid(count);
    }
    if (!brokers.every(broker => isValidHostPort(broker))) {
        invalid(
            `${fieldPath}.Broker must be host:port: a host name, an IPv4 address or an IPv6 address in ` +
                'brackets, then a port from 1 to 65535, with no comma and no whitespace.',
        );
    }
    if (brokers.some(broker => isLocalAddress(broker))) {
        invalid(
            `${fieldPath}.Broker must not be a loopback, link-local or unspecified address, ` +
                'or the name localhost or a name under .localhost.',
        );
    }
    return brokers.slice();
}

function readKafkaConnection(node) {
    const path = 'Connection';
    const block = readBlock(node, path, ['Brokers', 'Topic', 'RequiredAcks', 'CompressionType']);
    const connection = {
        Brokers: readBrokers(block, path),
    };
    const topic = readString(block, path, 'Topic', true);
    if (!isValidKafkaTopic(topic)) {
        invalid(`${path}.Topic must have 1 to 249 letters, digits, dots, underscores or hyphens, and not be . or ..`);
    }
    connection.Topic = topic;
    const requiredAcks = readEnum(block, path, 'RequiredAcks', requiredAcksValues, false);
    connection.RequiredAcks = requiredAcks === undefined ? defaultRequiredAcks : Number(requiredAcks);
    const compressionType = readEnum(block, path, 'CompressionType', compressionTypes, false);
    connection.CompressionType = compressionType === undefined ? defaultCompressionType : compressionType;
    return connection;
}

// The Connection rules of each Type.
function readConnection(type, node) {
    switch (type) {
        case 'kafka':
            return readKafkaConnection(node);
        default:
            return invalid(`NotificationDestination.Type must be one of ${types.join(', ')}.`);
    }
}

function readSasl(node) {
    const path = 'Auth.Sasl';
    const block = readBlock(node, path, ['Mechanism', 'Username', 'Password']);
    const mechanism = readString(block, path, 'Mechanism', true);
    if (mechanism === 'GSSAPI') {
        invalid(`${path}.Mechanism GSSAPI is not supported: use ${saslMechanisms.join(', ')}.`);
    }
    if (!saslMechanisms.includes(mechanism)) {
        invalid(`${path}.Mechanism must be one of ${saslMechanisms.join(', ')}.`);
    }
    return {
        Mechanism: mechanism,
        Username: readString(block, path, 'Username', true),
        Password: readString(block, path, 'Password', true),
    };
}

function readTls(node, authType) {
    const path = 'Auth.Tls';
    const { allowed, required } = tlsFields[authType];
    const block = readBlock(node, path, tlsFields.mtls.allowed);
    Object.keys(block).forEach(name => {
        if (!allowed.includes(name)) {
            invalid(`${path}.${name} does not apply to Auth.Type ${authType}.`);
        }
    });
    const tls = {};
    allowed.forEach(name => {
        const value = readString(block, path, name, required.includes(name));
        if (value !== undefined) {
            tls[name] = value;
        }
    });
    return tls;
}

function readAuth(node) {
    const path = 'Auth';
    const block = readBlock(node, path, ['Type', 'Sasl', 'Tls']);
    const type = readEnum(block, path, 'Type', authTypes, true);
    const auth = { Type: type };
    if (saslAuthTypes.includes(type)) {
        if (block.Sasl === undefined) {
            invalid(`${path}.Sasl is required with Auth.Type ${type}.`);
        }
        auth.Sasl = readSasl(block.Sasl);
    } else if (block.Sasl !== undefined) {
        invalid(`${path}.Sasl does not apply to Auth.Type ${type}.`);
    }
    if (tlsFields[type] === undefined) {
        if (block.Tls !== undefined) {
            invalid(`${path}.Tls does not apply to Auth.Type ${type}.`);
        }
    } else if (block.Tls !== undefined) {
        auth.Tls = readTls(block.Tls, type);
    } else if (tlsFields[type].blockRequired) {
        invalid(`${path}.Tls is required with Auth.Type ${type}.`);
    }
    return auth;
}

/**
 * Check a destination body and return its record fields.
 * @param {object} parsedXml - the body as parseXML gives it
 * @return {object} - { error } with an InvalidDestination error, or
 * { Type, Connection, Auth }
 */
function parseDestination(parsedXml) {
    try {
        if (parsedXml === null || typeof parsedXml !== 'object' || !('NotificationDestination' in parsedXml)) {
            invalid('The root element must be NotificationDestination.');
        }
        const root = readBlock(parsedXml.NotificationDestination, 'NotificationDestination', [
            'Type',
            'Connection',
            'Auth',
        ]);
        const type = readEnum(root, 'NotificationDestination', 'Type', types, true);
        if (root.Connection === undefined) {
            invalid('Connection is required.');
        }
        const connection = readConnection(type, root.Connection);
        if (root.Auth === undefined) {
            invalid('Auth is required.');
        }
        const auth = readAuth(root.Auth);
        return { Type: type, Connection: connection, Auth: auth };
    } catch (err) {
        if (err instanceof InvalidField) {
            return { error: errorInstances.InvalidDestination.customizeDescription(err.message) };
        }
        throw err;
    }
}

module.exports = parseDestination;
