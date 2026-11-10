const { storage } = require('@scality/arsenal');
const AuthInfo = require('@scality/arsenal').auth.AuthInfo;

const recordStore = require('../../../lib/api/apiUtils/notificationDestination/recordStore');
const { config } = require('../../../lib/Config');
const constants = require('../../../constants');
const { cleanup, makeAuthInfo } = require('../helpers');

const { metadata } = storage.metadata.inMemory.metadata;
const { accountResourcesBucket: recordsBucket, notificationDestinationKeyTag: tag, splitter } = constants;

// the account of accessKey1 in makeAuthInfo
const accountId = '123456789098';

const keyOf = name => `${accountId}${splitter}${tag}${splitter}${name}`;

const accountAuthInfo = makeAuthInfo('accessKey1');
const otherAuthInfo = makeAuthInfo('accessKey2');

// An identity of the account whose short ID is not the account ID, as Vault
// gives for an IAM user or a role. makeAuthInfo gives a user the short ID of
// its account.
function makeIdentity(arn, shortid) {
    return new AuthInfo({ canonicalID: accountAuthInfo.getCanonicalID(), shortid, arn });
}

const userAuthInfo = makeIdentity(`arn:aws:iam::${accountId}:user/alice`, 'AIDAUSERID0000000000');

// A request for one destination name, as the route gives it to a handler.
function makeRequest(method, name, post) {
    return {
        method,
        url: `/?notification-destination&name=${encodeURIComponent(name)}`,
        query: { 'notification-destination': '', name },
        headers: { host: 's3.amazonaws.com' },
        post,
        actionImplicitDenies: false,
    };
}

// Resolve with the callback arguments, so that a failed assertion fails the
// test at once instead of in a metadata callback.
function call(handler, authInfo, request, log) {
    return new Promise(resolve => handler(authInfo, request, log, (...args) => resolve(args)));
}

// The in-memory store keeps the value it is given. Store a copy, so that a
// handler that edited the stored record in place could not edit the
// expected one too.
function seed(record, log) {
    return new Promise((resolve, reject) =>
        recordStore.putDestination(accountId, record.Name, structuredClone(record), log, err =>
            err ? reject(err) : resolve(),
        ),
    );
}

function storedRecord(name) {
    const keys = metadata.keyMaps.get(recordsBucket);
    return keys ? keys.get(keyOf(name)) : undefined;
}

// Run the record store on the in-memory store, whatever S3BACKEND says, and
// start from an empty store. Return the function that stops the record
// store, empties the in-memory store and gives the metadata backend back:
// stop() drops its client, so that the next test starts a new one.
function useMemRecordStore() {
    const savedBackend = config.backends.metadata;
    config.backends.metadata = 'mem';
    recordStore.stop();
    cleanup();
    return () => {
        recordStore.stop();
        cleanup();
        config.backends.metadata = savedBackend;
    };
}

module.exports = {
    accountId,
    keyOf,
    accountAuthInfo,
    otherAuthInfo,
    makeIdentity,
    userAuthInfo,
    makeRequest,
    call,
    seed,
    storedRecord,
    useMemRecordStore,
};
