RATIONALE
=========
The bucket API will be used for managing buckets behind the S3 interface.

We plan to have only 2 backends using this interface:
* One production backend that uses raft and leveldb (defined here: https://github.com/scality/IronMan-MetaData/blob/master/Design.md)
* One debug backend purely in memory

One important remark here is that we don't want an abstraction but a duck-typing style interface (different classes MemoryBucket and Bucket having the same methods PUTObject(), GETObject(), etc).

Notes about the memory backend: The backend is currently a simple key/value store in memory. The functions actually use nextTick() to emulate the future asynchronous behavior of the production backend.



BUCKET API
==========

The bucket API is a very simple API with 4 functions:

PUTObject(): add a key in the bucket
GETObject(): get a key from the bucket
DELETEObject(): delete a key from the bucket
GETBucketListObjects(): perform the complex bucket listing AWS search function with various flavors. This function returns a response in a ListBucketResult object.

GETBucketListObjects(prefix, marker, delimiter, maxKeys, callback) behavior is the following:

prefix (not required): Limits the response to keys that begin with the specified prefix. You can use prefixes to separate a bucket into different groupings of keys. (You can think of using prefix to make groups in the same way you'd use a folder in a file system.)

marker (not required): Specifies the key to start with when listing objects in a bucket. Amazon S3 returns object keys in alphabetical order, starting with key after the marker in order.

delimiter (not required): A delimiter is a character you use to group keys.
All keys that contain the same string between the prefix, if specified, and the first occurrence of the delimiter after the prefix are grouped under a single result element, CommonPrefixes. If you don't specify the prefix parameter, then the substring starts at the beginning of the key. The keys that are grouped under CommonPrefixes are not returned elsewhere in the response.

maxKeys: Sets the maximum number of keys returned in the response body. You can add this to your request if you want to retrieve fewer than the default 1000 keys. 
The response might contain fewer keys but will never contain more. If there are additional keys that satisfy the search criteria but were not returned because maxKeys was exceeded, the response contains an attribute of IsTruncated set to true and a NextMarker. To return the additional keys, call the function again using NextMarker as your marker argument in the function.  

Any key that does not contain the delimiter will be returned individually in Contents rather than in CommonPrefixes.  

If there is an error, the error subfield is returned in the response.

