const Common = require('../common');

const kms = [];
let count = 1;

const backend = {
    /*
     * Target implementation will be async. let's mimic it
     */

    supportsDefaultKeyPerAccount: false,

     /**
      *
      * @param {BucketInfo} bucket - bucket info
      * @param {object} log - logger object
      * @param {function} cb - callback
      * @returns {undefined}
      * @callback called with (err, masterKeyId: string)
      */
    createBucketKey: function createBucketKeyMem(bucket, log, cb) {
        process.nextTick(() => {
            // Using createDataKey here for purposes of createBucketKeyMem
            // so that we do not need a separate function.
            kms[count] = Common.createDataKey();
            cb(null, (count++).toString());
        });
    },

    /**
     *
     * @param {string} bucketKeyId - the Id of the bucket key
     * @param {object} log - logger object
     * @param {function} cb - callback
     * @returns {undefined}
     * @callback called with (err)
     */
    destroyBucketKey: function destroyBucketKeyMem(bucketKeyId, log, cb) {
        process.nextTick(() => {
            kms[bucketKeyId] = undefined;
            cb(null);
        });
    },

     /**
      *
      * @param {number} cryptoScheme - crypto scheme version number
      * @param {string} masterKeyId - key to retrieve master key
      * @param {buffer} plainTextDataKey - data key
      * @param {object} log - logger object
      * @param {function} cb - callback
      * @returns {undefined}
      * @callback called with (err, cipheredDataKey: Buffer)
      */
    cipherDataKey: function cipherDataKeyMem(cryptoScheme,
                                             masterKeyId,
                                             plainTextDataKey,
                                             log,
                                             cb) {
        process.nextTick(() => {
            Common.createCipher(
                cryptoScheme, kms[masterKeyId], 0, log,
                (err, cipher) => {
                    if (err) {
                        cb(err);
                        return;
                    }
                    let cipheredDataKey =
                            cipher.update(plainTextDataKey);
                    // call final() to ensure that any bytes remaining in
                    // the output of the stream are captured
                    const final = cipher.final();
                    if (final.length !== 0) {
                        cipheredDataKey =
                            Buffer.concat([cipheredDataKey,
                                final]);
                    }
                    cb(null, cipheredDataKey);
                });
        });
    },

     /**
      *
      * @param {number} cryptoScheme - crypto scheme version number
      * @param {string} masterKeyId - key to retrieve master key
      * @param {buffer} cipheredDataKey - data key
      * @param {object} log - logger object
      * @param {function} cb - callback
      * @returns {undefined}
      * @callback called with (err, plainTextDataKey: Buffer)
      */
    decipherDataKey: function decipherDataKeyMem(cryptoScheme,
                                                 masterKeyId,
                                                 cipheredDataKey,
                                                 log,
                                                 cb) {
        process.nextTick(() => {
            Common.createDecipher(
                cryptoScheme, kms[masterKeyId], 0, log,
                (err, decipher) => {
                    if (err) {
                        cb(err);
                        return;
                    }
                    let plainTextDataKey =
                            decipher.update(cipheredDataKey);
                    const final = decipher.final();
                    if (final.length !== 0) {
                        plainTextDataKey =
                            Buffer.concat([plainTextDataKey,
                                final]);
                    }
                    cb(null, plainTextDataKey);
                });
        });
    },

};

module.exports = {
    kms,
    backend,
};
