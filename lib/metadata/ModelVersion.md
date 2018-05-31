# BucketInfo Model Version History

## Model Version 0/1

### Properties

``` javascript
this._acl = aclInstance;
this._name = name;
this._owner = owner;
this._ownerDisplayName = ownerDisplayName;
this._creationDate = creationDate;
```

### Usage

No explicit references in the code since mdBucketModelVersion
property not added until Model Version 2

## Model Version 2

### Properties Added

``` javascript
this._mdBucketModelVersion = mdBucketModelVersion || 0
this._transient = transient || false;
this._deleted = deleted || false;
```

### Usage

Used to determine which splitter to use ( < 2 means old splitter)

## Model version 3

### Properties Added

```
this._serverSideEncryption = serverSideEncryption || null;
```

### Usage

Used to store the server bucket encryption info

## Model version 4

### Properties Added

```javascript
this._locationConstraint = LocationConstraint || null;
```

### Usage

Used to store the location constraint of the bucket

## Model version 5

### Properties Added

```javascript
this._websiteConfiguration = websiteConfiguration || null;
this._cors = cors || null;
```

### Usage

Used to store the bucket website configuration info
and to store CORS rules to apply to cross-domain requests

## Model version 6

### Properties Added

```javascript
this._lifecycleConfiguration = lifecycleConfiguration || null;
```

### Usage

Used to store the bucket lifecycle configuration info.
Backwards compatible: add a uid to bucket if not exist. Otherwise, use existing

## Model version 7

### Properties Added

```javascript
this._uid = uid || uuid();
```

### Usage

Used to set a unique identifier on a bucket

## Model version 8

### Properties Added

```javascript
this._readLocationConstraint = readLocationConstraint || null;
```

### Usage

Used to store default read location of the bucket

## Model version 9

### Properties Added

```javascript
this._isNFS = isNFS || null;
```

### Usage

Used to determine whether the bucket is on NFS

