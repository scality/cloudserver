const { ipCheck } = require('@scality/arsenal');

/**
 * Plain checks of the network names that a destination holds. Every place
 * that reads such a value should share these checks. Thus a broker address
 * means the same everywhere.
 */

const minPort = 1;
const maxPort = 65535;
const maxHostnameLength = 253;
// One to 63 letters, digits or hyphens, with no hyphen first or last.
const hostnameLabel = '[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?';
const hostnamePattern = new RegExp(`^${hostnameLabel}(?:\\.${hostnameLabel})*$`);
// Decimal octets with no leading zero, which a resolver would read as octal.
const octet = '(?:0|[1-9][0-9]{0,2})';
const dottedQuad = new RegExp(`^${octet}(?:\\.${octet}){3}$`);
// A last label that a resolver may read as a number: digits, or hex.
const numericLabel = /^(?:0x[0-9a-f]*|[0-9]+)$/i;
const portPattern = /^[0-9]{1,5}$/;
// The names that Kafka accepts for a topic, other than . and ..
const kafkaTopicPattern = /^[a-zA-Z0-9._-]{1,249}$/;

function isIpAddress(text) {
    return typeof ipCheck.parseIp(text).kind === 'function';
}

/**
 * A host name, an IPv4 address, or an IPv6 address in brackets.
 * @param {string} host - the host part of a host:port entry
 * @return {boolean} - true when the host is valid
 */
function isValidHost(host) {
    if (host.startsWith('[') && host.endsWith(']')) {
        const address = host.slice(1, -1);
        // A zone ID (%eth0) means something on this host only.
        if (!address.includes(':') || address.includes('%')) {
            return false;
        }
        // An embedded IPv4 address follows the dotted-quad rule too.
        const embedded = address.match(/[0-9.]+$/)?.[0];
        if (embedded?.includes('.') && !dottedQuad.test(embedded)) {
            return false;
        }
        return isIpAddress(address);
    }
    // Digits and dots only: an IPv4 address. Accept only the dotted-quad
    // form, because parseIp also takes short forms such as 1.2.3.
    if (/^[0-9.]+$/.test(host)) {
        return dottedQuad.test(host) && isIpAddress(host);
    }
    // A host name whose last label reads as a number is an IPv4 address in
    // another form, such as 0x7f.0.0.1. Refuse it.
    const lastLabel = host.slice(host.lastIndexOf('.') + 1);
    return host.length <= maxHostnameLength && hostnamePattern.test(host) && !numericLabel.test(lastLabel);
}

/**
 * A host:port entry: a host name, an IPv4 address or an IPv6 address in
 * brackets, then a colon and a port from 1 to 65535. No comma, no whitespace.
 * @param {*} entry - the value to check
 * @return {boolean} - true when the entry is valid
 */
function isValidHostPort(entry) {
    if (typeof entry !== 'string') {
        return false;
    }
    const separator = entry.lastIndexOf(':');
    if (separator <= 0) {
        return false;
    }
    const port = entry.slice(separator + 1);
    if (!portPattern.test(port) || Number(port) < minPort || Number(port) > maxPort) {
        return false;
    }
    return isValidHost(entry.slice(0, separator));
}

// The address ranges that name this host or its link rather than a broker
// of the tenant: loopback, link-local (such as 169.254.169.254) and
// unspecified. Private ranges stay allowed, since brokers live there.
const localRanges = ['loopback', 'linkLocal', 'unspecified'];

// The host names that always name this host: localhost, and any name
// under .localhost (RFC 6761).
const localHostName = /^(?:.+\.)?localhost$/;

/**
 * Whether the host of a host:port entry is a literal loopback, link-local
 * or unspecified address, or the name localhost or a name under .localhost.
 * The name check ignores case and one trailing dot. This function never
 * resolves a host name. The address that another host name resolves to is
 * for the delivery side to guard.
 * @param {string} entry - a host:port entry
 * @return {boolean} - true when the host is such an address or name
 */
function isLocalAddress(entry) {
    let host = entry.slice(0, entry.lastIndexOf(':'));
    if (localHostName.test(host.toLowerCase().replace(/\.$/, ''))) {
        return true;
    }
    if (host.startsWith('[')) {
        host = host.slice(1, -1);
    }
    // parseIp gives an IPv4-mapped IPv6 address, in any form such as
    // ::ffff:7f00:1, as its IPv4 address. Thus the IPv4 ranges apply to it.
    const address = ipCheck.parseIp(host);
    return typeof address.range === 'function' && localRanges.includes(address.range());
}

/**
 * A Kafka topic name: 1 to 249 letters, digits, dots, underscores or
 * hyphens, other than . and ..
 * @param {*} name - the value to check
 * @return {boolean} - true when the name is valid
 */
function isValidKafkaTopic(name) {
    return typeof name === 'string' && kafkaTopicPattern.test(name) && name !== '.' && name !== '..';
}

module.exports = {
    isLocalAddress,
    isValidHostPort,
    isValidKafkaTopic,
};
