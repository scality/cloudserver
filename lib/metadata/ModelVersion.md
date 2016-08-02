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