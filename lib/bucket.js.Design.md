The bucket API is a very simple API with 4 functions:
* putKey(): add a key in the bucket
* getKey(): get a key in the bucket
* delKey(): delete a key from the bucket
* getPrefix(): perform the complex getPrefix AWS search function with various flavors

The backend is currently a simple key/value store in memory. The functions actually use nextTick() to emulate their
future asynchronous behavior when it will be leveldb, cassandra, etc.



