const { s3middleware } = require('@scality/arsenal');

const { escapeForXml } = s3middleware;

// A record that lacks a value cannot be answered. Throw, rather than give
// the text undefined. A number, such as RequiredAcks, becomes its text
// before the escape, because the escape takes strings.
function element(name, value) {
    if (value === undefined || value === null) {
        throw new Error(`the destination record lacks ${name}`);
    }
    return `<${name}>${escapeForXml(String(value))}</${name}>`;
}

function connectionXml(connection) {
    return [
        '<Connection>',
        '<Brokers>',
        ...connection.Brokers.map(broker => element('Broker', broker)),
        '</Brokers>',
        element('Topic', connection.Topic),
        element('RequiredAcks', connection.RequiredAcks),
        element('CompressionType', connection.CompressionType),
        '</Connection>',
    ].join('');
}

/**
 * Return the NotificationDestination element of a stored destination
 * record, as Get answers it and as List answers each entry. The element
 * names each field that it gives. Thus a field that the record gains never
 * leaks by default. This function reduces Auth to its Type, because the
 * Sasl and Tls blocks hold secrets. These blocks never leave in an answer.
 * @param {object} record - the destination record, as the record store
 * gives it
 * @return {string} - the element, with no XML declaration and no xmlns
 */
function destinationXml(record) {
    return [
        '<NotificationDestination>',
        element('Name', record.Name),
        element('Arn', record.Arn),
        element('AccountId', record.AccountId),
        element('Type', record.Type),
        connectionXml(record.Connection),
        '<Auth>',
        element('Type', record.Auth.Type),
        '</Auth>',
        element('CreatedAt', record.CreatedAt),
        element('UpdatedAt', record.UpdatedAt),
        '</NotificationDestination>',
    ].join('');
}

module.exports = {
    destinationXml,
};
