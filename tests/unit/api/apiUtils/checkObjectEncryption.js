const assert = require('assert');

const checkObjectEncryption
    = require('../../../../lib/api/apiUtils/object/checkEncryption');

describe('object util checkEncryption.isValidSSES3', () => {
    it('should respond true if server side encryption matches object '
        + 'encryption request', () => {
        const serverSideEncryption = {
            algorithm: 'AES256',
        };

        const request = {
            headers: {
                'x-amz-server-side-encryption': 'AES256',
            },
        };

        const isValidSSES3
            = checkObjectEncryption.isValidSSES3(request, serverSideEncryption);
        assert.strictEqual(isValidSSES3, true);
    });

    it('should respond true if there is no bucket encryption and '
        + 'no encryption header in object request', () => {
        const request = {
            headers: {},
        };

        const isValidSSES3
            = checkObjectEncryption.isValidSSES3(request, null);
        assert.strictEqual(isValidSSES3, true);
    });

    it('should respond true if there is no encryption header '
        + 'and bucket encryption algorithm is AES256', () => {
        const serverSideEncryption = {
            algorithm: 'AES256',
        };
        const request = {
            headers: {},
        };

        const isValidSSES3
            = checkObjectEncryption.isValidSSES3(request, serverSideEncryption);
        assert.strictEqual(isValidSSES3, true);
    });

    it('should respond false if there is encryption header in object request '
        + 'and no bucket encryption', () => {
        const request = {
            headers: {
                'x-amz-server-side-encryption': 'AES256',
            },
        };

        const isValidSSES3
            = checkObjectEncryption.isValidSSES3(request, null);
        assert.strictEqual(isValidSSES3, false);
    });

    it('should respond false if the encryption header value is not AES256 '
        + 'and bucket encryption algorithm is AES256', () => {
        const serverSideEncryption = {
            algorithm: 'AES256',
        };
        const request = {
            headers: {
                'x-amz-server-side-encryption': 'rsa',
            },
        };

        const isValidSSES3
            = checkObjectEncryption.isValidSSES3(request, serverSideEncryption);
        assert.strictEqual(isValidSSES3, false);
    });

    it('should respond false if the encryption header value is AES256 '
        + 'and bucket encryption algorithm is not AES256', () => {
        const serverSideEncryption = {
            algorithm: 'rsa',
        };
        const request = {
            headers: {
                'x-amz-server-side-encryption': 'AES256',
            },
        };

        const isValidSSES3
            = checkObjectEncryption.isValidSSES3(request, serverSideEncryption);
        assert.strictEqual(isValidSSES3, false);
    });
});

