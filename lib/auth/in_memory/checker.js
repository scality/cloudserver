import Logger from 'werelogs';

// Here, we expect the logger to have already been configured in S3
const log = new Logger('S3');

function incr(count) {
    if (count !== undefined) {
        return count + 1;
    }
    return 1;
}

/**
 * This function ensures that the field `name` inside `container` is of the
 * expected `type` inside `obj`. If any error is found, an entry is added into
 * the error collector object.
 *
 * @param {object} data - the error collector object
 * @param {string} container - the name of the entity that contains
 *                             what we're checking
 * @param {string} name - the name of the entity we're checking for
 * @param {string} type - expected typename of the entity we're checking
 * @param {object} obj - the object we're checking the fields of
 * @return {boolean} true if the type is Ok and no error found
 *                   false if an error was found and reported
 */
function checkType(data, container, name, type, obj) {
    if ((type === 'array' && !Array.isArray(obj[name]))
        || (type !== 'array' && typeof obj[name] !== type)) {
        data.errors.push({
            txt: 'property is not of the expected type',
            obj: {
                entity: container,
                property: name,
                type: typeof obj[name],
                expectedType: type,
            },
        });
        return false;
    }
    return true;
}

/**
 * This function ensures that the field `name` inside `obj` which is a
 * `container`. If any error is found, an entry is added into the error
 * collector object.
 *
 * @param {object} data - the error collector object
 * @param {string} container - the name of the entity that contains
 *                             what we're checking
 * @param {string} name - the name of the entity we're checking for
 * @param {string} type - expected typename of the entity we're checking
 * @param {object} obj - the object we're checking the fields of
 * @return {boolean} true if the field exists and type is Ok
 *                   false if an error was found and reported
 */
function checkExists(data, container, name, type, obj) {
    if (obj[name] === undefined) {
        data.errors.push({
            txt: 'missing property in auth entity',
            obj: {
                entity: container,
                property: name,
            },
        });
        return false;
    }
    return checkType(data, container, name, type, obj);
}

function checkUser(data, userObj) {
    if (checkExists(data, 'User', 'arn', 'string', userObj)) {
        // eslint-disable-next-line no-param-reassign
        data.arns[userObj.arn] = incr(data.arns[userObj.arn]);
    }
    if (checkExists(data, 'User', 'email', 'string', userObj)) {
        // eslint-disable-next-line no-param-reassign
        data.emails[userObj.email] = incr(data.emails[userObj.email]);
    }
    if (checkExists(data, 'User', 'keys', 'array', userObj)) {
        userObj.keys.forEach(keyObj => {
            // eslint-disable-next-line no-param-reassign
            data.keys[keyObj.access] = incr(data.keys[keyObj.access]);
        });
    }
}

function checkAccount(data, accountObj) {
    if (checkExists(data, 'Account', 'email', 'string', accountObj)) {
        // eslint-disable-next-line no-param-reassign
        data.emails[accountObj.email] = incr(data.emails[accountObj.email]);
    }
    if (checkExists(data, 'Account', 'arn', 'string', accountObj)) {
        // eslint-disable-next-line no-param-reassign
        data.arns[accountObj.arn] = incr(data.arns[accountObj.arn]);
    }
    if (checkExists(data, 'Account', 'canonicalID', 'string', accountObj)) {
        // eslint-disable-next-line no-param-reassign
        data.canonicalIds[accountObj.canonicalID] =
            incr(data.canonicalIds[accountObj.canonicalID]);
    }

    if (accountObj.users) {
        if (checkType(data, 'Account', 'users', 'array', accountObj)) {
            accountObj.users.forEach(userObj => checkUser(data, userObj));
        }
    }

    if (accountObj.keys) {
        if (checkType(data, 'Account', 'keys', 'array', accountObj)) {
            accountObj.keys.forEach(keyObj => {
                // eslint-disable-next-line no-param-reassign
                data.keys[keyObj.access] = incr(data.keys[keyObj.access]);
            });
        }
    }
}

function dumpCountError(property, obj) {
    let count = 0;
    Object.keys(obj).forEach(key => {
        if (obj[key] > 1) {
            log.error('property should be unique', {
                property,
                value: key,
                count: obj[key],
            });
            ++count;
        }
    });
    return count;
}

function dumpErrors(checkData) {
    let nerr = dumpCountError('CanonicalID', checkData.canonicalIds);
    nerr += dumpCountError('Email', checkData.emails);
    nerr += dumpCountError('ARN', checkData.arns);
    nerr += dumpCountError('AccessKey', checkData.keys);

    if (checkData.errors.length > 0) {
        checkData.errors.forEach(msg => {
            log.error(msg.txt, msg.obj);
        });
    }

    if (checkData.errors.length === 0 && nerr === 0) {
        return false;
    }

    log.fatal('invalid authentication config file (cannot start)');

    return true;
}

/**
 * @param {object} authdata - the authentication config file's data
 * @return {boolean} true on erroneous data
 *                   false on success
 */
export default function check(authdata) {
    const checkData = {
        errors: [],
        emails: [],
        arns: [],
        canonicalIds: [],
        keys: [],
    };

    if (authdata.accounts === undefined) {
        checkData.errors.push({
            txt: 'no "accounts" array defined in Auth config',
        });
        return dumpErrors(checkData);
    }

    authdata.accounts.forEach(account => {
        checkAccount(checkData, account);
    });

    return dumpErrors(checkData);
}
