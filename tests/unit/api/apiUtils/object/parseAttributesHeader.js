const assert = require('assert');

const parseAttributesHeaders = require('../../../../../lib/api/apiUtils/object/parseAttributesHeader');

describe('parseAttributesHeaders', () => {
  describe('missing or empty header', () => {
    it('should throw InvalidRequest error when header is missing', () => {
      const headers = {};

      assert.throws(
        () => parseAttributesHeaders(headers),
        err => {
          assert(err.is);
          assert.strictEqual(err.is.InvalidRequest, true);
          assert.strictEqual(
              err.description,
              'The x-amz-object-attributes header specifying the attributes to be retrieved is either missing or empty',
          );
          return true;
        },
      );
    });

    it('should throw InvalidArgument error when header is empty string', () => {
      const headers = { 'x-amz-object-attributes': '' };

      assert.throws(
        () => parseAttributesHeaders(headers),
        err => {
          assert(err.is);
          assert.strictEqual(err.is.InvalidArgument, true);
          assert.strictEqual(err.description, 'Invalid attribute name specified.');
          return true;
        },
      );
    });

    it('should throw InvalidArgument error when header contains only whitespace', () => {
      const headers = { 'x-amz-object-attributes': '   ' };

      assert.throws(
        () => parseAttributesHeaders(headers),
        err => {
          assert(err.is);
          assert.strictEqual(err.is.InvalidArgument, true);
          assert.strictEqual(err.description, 'Invalid attribute name specified.');
          return true;
        },
      );
    });

    it('should throw InvalidArgument error when header contains only commas', () => {
      const headers = { 'x-amz-object-attributes': ',,,' };

      assert.throws(
        () => parseAttributesHeaders(headers),
        err => {
          assert(err.is);
          assert.strictEqual(err.is.InvalidArgument, true);
          assert.strictEqual(err.description, 'Invalid attribute name specified.');
          return true;
        },
      );
    });
  });

  describe('invalid attribute names', () => {
    it('should throw InvalidArgument error for single invalid attribute', () => {
      const headers = { 'x-amz-object-attributes': 'InvalidAttribute' };

      assert.throws(
        () => parseAttributesHeaders(headers),
        err => {
          assert(err.is);
          assert.strictEqual(err.is.InvalidArgument, true);
          assert.strictEqual(err.description, 'Invalid attribute name specified.');
          return true;
        },
      );
    });

    it('should throw InvalidArgument error when one attribute is invalid among valid ones', () => {
      const headers = { 'x-amz-object-attributes': 'ETag,InvalidAttribute,ObjectSize' };

      assert.throws(
        () => parseAttributesHeaders(headers),
        err => {
          assert(err.is);
          assert.strictEqual(err.is.InvalidArgument, true);
          assert.strictEqual(err.description, 'Invalid attribute name specified.');
          return true;
        },
      );
    });

    it('should throw InvalidArgument error for multiple invalid attributes', () => {
      const headers = { 'x-amz-object-attributes': 'Invalid1,Invalid2' };

      assert.throws(
        () => parseAttributesHeaders(headers),
        err => {
          assert(err.is);
          assert.strictEqual(err.is.InvalidArgument, true);
          assert.strictEqual(err.description, 'Invalid attribute name specified.');
          return true;
        },
      );
    });
  });

  describe('valid attribute names', () => {
    it('should return array with single valid attribute ETag', () => {
      const headers = { 'x-amz-object-attributes': 'ETag' };
      const result = parseAttributesHeaders(headers);

      assert(Array.isArray(result));
      assert.deepStrictEqual(result, ['ETag']);
    });

    it('should return array with single valid attribute StorageClass', () => {
      const headers = { 'x-amz-object-attributes': 'StorageClass' };
      const result = parseAttributesHeaders(headers);

      assert(Array.isArray(result));
      assert.deepStrictEqual(result, ['StorageClass']);
    });

    it('should return array with single valid attribute ObjectSize', () => {
      const headers = { 'x-amz-object-attributes': 'ObjectSize' };
      const result = parseAttributesHeaders(headers);

      assert(Array.isArray(result));
      assert.deepStrictEqual(result, ['ObjectSize']);
    });

    it('should return array with single valid attribute ObjectParts', () => {
      const headers = { 'x-amz-object-attributes': 'ObjectParts' };
      const result = parseAttributesHeaders(headers);

      assert(Array.isArray(result));
      assert.deepStrictEqual(result, ['ObjectParts']);
    });

    it('should return array with single valid attribute Checksum', () => {
      const headers = { 'x-amz-object-attributes': 'Checksum' };
      const result = parseAttributesHeaders(headers);

      assert(Array.isArray(result));
      assert.deepStrictEqual(result, ['Checksum']);
    });

    it('should return array with multiple valid attributes', () => {
      const headers = { 'x-amz-object-attributes': 'ETag,ObjectSize,StorageClass' };
      const result = parseAttributesHeaders(headers);

      assert(Array.isArray(result));
      assert.deepStrictEqual(result, ['ETag', 'ObjectSize', 'StorageClass']);
    });

    it('should return array with all valid attributes', () => {
      const headers = { 'x-amz-object-attributes': 'StorageClass,ObjectSize,ObjectParts,Checksum,ETag' };
      const result = parseAttributesHeaders(headers);

      assert(Array.isArray(result));
      assert.strictEqual(result.length, 5);
      assert(result.includes('StorageClass'));
      assert(result.includes('ObjectSize'));
      assert(result.includes('ObjectParts'));
      assert(result.includes('Checksum'));
      assert(result.includes('ETag'));
    });
  });

  describe('whitespace handling', () => {
    it('should trim whitespace around attribute names', () => {
      const headers = { 'x-amz-object-attributes': ' ETag , ObjectSize ' };
      const result = parseAttributesHeaders(headers);

      assert(Array.isArray(result));
      assert.deepStrictEqual(result, ['ETag', 'ObjectSize']);
    });

    it('should throw InvalidArgument for extra commas between attributes', () => {
      const headers = { 'x-amz-object-attributes': 'ETag,,ObjectSize' };

      assert.throws(
        () => parseAttributesHeaders(headers),
        err => {
          assert(err.is);
          assert.strictEqual(err.is.InvalidArgument, true);
          assert.strictEqual(err.description, 'Invalid attribute name specified.');
          return true;
        },
      );
    });

    it('should throw InvalidArgument for leading and trailing commas', () => {
      const headers = { 'x-amz-object-attributes': ',ETag,ObjectSize,' };

      assert.throws(
        () => parseAttributesHeaders(headers),
        err => {
          assert(err.is);
          assert.strictEqual(err.is.InvalidArgument, true);
          assert.strictEqual(err.description, 'Invalid attribute name specified.');
          return true;
        },
      );
    });
  });
});
