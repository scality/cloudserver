const Common = require('../common');

const backend = {
    /*
     * Target implementation will be async. let's mimic it
     */


    /**
     *
     * @param {string} bucketName - bucket name
     * @param {object} log - logger object
     * @param {function} cb - callback
     * @returns {undefined}
     * @callback called with (err, masterKeyId: string)
    */
    createBucketKey: function createBucketKeyMem(bucketName, log, cb) {
        process.nextTick(() => {
            // Using createDataKey here for purposes of createBucketKeyMem
            // so that we do not need a separate function.
            const newKey = Common.createDataKey().toString('hex');
            cb(null, newKey);
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
            /* this is a no-op since actual key is stored
             * along with the bucket attributes
             */
            cb(null);
        });
    },

     /**
      *
      * @param {number} cryptoScheme - crypto scheme version number
      * @param {string} masterKeyId - master key; for the file backend
      * the master key is the actual bucket master key rather than the key to
      * retrieve the actual key from a dictionary
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
            const masterKey = Buffer.from(masterKeyId, 'hex');
            Common.createCipher(
                cryptoScheme, masterKey, 0, log,
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
      * @param {string} masterKeyId - master key; for the file backend
      * the master key is the actual bucket master key rather than the key to
      * retrieve the actual key from a dictionary
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
            const masterKey = Buffer.from(masterKeyId, 'hex');
            Common.createDecipher(
                cryptoScheme, masterKey, 0, log,
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

module.exports = backend;
