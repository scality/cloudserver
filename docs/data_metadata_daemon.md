# Data-metadata daemon Architecture and Operational guide

This document presents the architecture of the data-metadata daemon
(dmd) used for the community edition of S3 server. It also provides a
guide on how to operate it.

The dmd is responsible for storing and retrieving S3 data and
metadata, and is accessed by S3 connectors through socket.io
(metadata) and REST (data) APIs.

It has been designed such that more than one S3 connector can access
the same buckets by communicating with the dmd. It also means that the
dmd can be hosted on a separate container or machine.

## Operation

### Startup

The simplest deployment is still to launch with npm start, this will
start one instance of the S3 connector and will listen on the locally
bound dmd ports 9990 and 9991 (by default, see below).

The dmd can be started independently from the S3 server by running
this command in the S3 directory:

    npm run start_dmd

This will open two ports:

* one is based on socket.io and is used for metadata transfers (9990
  by default)

* the other is a REST interface used for data transfers (9991 by
  default)

Then, one or more instances of S3 server without the dmd can be
started elsewhere with:

    npm run start_s3server

### Configuration

Most configuration happens in `config.json` for S3 server, local
storage paths can be changed where the dmd is started using
environment variables, like before: `S3DATAPATH` and `S3METADATAPATH`.

In `config.json`, the following sections are used to configure access
to the dmd through separate configuration of the data and metadata
access:

    "metadataClient": {
        "host": "localhost",
        "port": 9990
    },
    "dataClient": {
        "host": "localhost",
        "port": 9991
    },

To run a remote dmd, you have to do the following:

* change both `"host"` attributes to the IP or host name where the dmd
  is run.

* Modify the `"bindAddress"` attributes in `"metadataDaemon"` and
  `"dataDaemon"` sections where the dmd is run to accept remote
  connections (e.g. `"::"`)

## Architecture

This section gives a bit more insight on how it works internally.

![./images/data_metadata_daemon_arch.png](./images/data_metadata_daemon_arch.png
 "Architecture diagram")

### Metadata on socket.io

This communication is based on an RPC system based on socket.io events
sent by S3 connectors, received by the DMD and acknowledged back to
the S3 connector.

The actual payload sent through socket.io is a JSON-serialized form of
the RPC call name and parameters, along with some additional
information like the request UIDs, and the sub-level information, sent
as object attributes in the JSON request.

With introduction of versioning support, the updates are now gathered
in the dmd for some number of milliseconds max, before being batched
as a single write to the database. This is done server-side, so the
API is meant to send individual updates.

Four RPC commands are available to clients: `put`, `get`, `del` and
`createReadStream`. They more or less map the parameters accepted by
the corresponding calls in the LevelUp implementation of LevelDB. They
differ in the following:

* The `sync` option is ignored (under the hood, puts are gathered into
  batches which have their `sync` property enforced when they are
  committed to the storage)

* Some additional versioning-specific options are supported

* `createReadStream` becomes asynchronous, takes an additional
  callback argument and returns the stream in the second callback
  parameter

Debugging the socket.io exchanges can be achieved by running the
daemon with `DEBUG='socket.io*'` environment variable set.

One parameter controls the timeout value after which RPC commands sent
end with a timeout error, it can be changed either:

* via the `DEFAULT_CALL_TIMEOUT_MS` option in `lib/network/rpc/rpc.js`

* or in the constructor call of the `MetadataFileClient` object (in
  `lib/metadata/bucketfile/backend.js` as `callTimeoutMs`.

Default value is 30000.

A specific implementation deals with streams, currently used for
listing a bucket. Streams emit `"stream-data"` events that pack one or
more items in the listing, and a special `“stream-end”` event when
done. Flow control is achieved by allowing a certain number of “in
flight” packets that have not received an ack yet (5 by default). Two
options can tune the behavior (for better throughput or getting it
more robust on weak networks), they have to be set in `mdserver.js`
file directly, as there is no support in `config.json` for now for
those options:

* `streamMaxPendingAck`: max number of pending ack events not yet
  received (default is 5)

* `streamAckTimeoutMs`: timeout for receiving an ack after an output
  stream packet is sent to the client (default is 5000)

### Data exchange through the REST data port

Data is read and written with REST semantic.

The web server recognizes a base path in the URL of `/DataFile` to be
a request to the data storage service.

#### PUT

A PUT on `/DataFile` URL and contents passed in the request body will
write a new object to the storage.

On success, a `201 Created` response is returned and the new URL to
the object is returned via the `Location` header (e.g.
`Location: /DataFile/50165db76eecea293abfd31103746dadb73a2074`).
The raw key can then be extracted simply by removing the leading
`/DataFile` service information from the returned URL.

#### GET

A GET is simply issued with REST semantic, e.g.:

    GET /DataFile/50165db76eecea293abfd31103746dadb73a2074 HTTP/1.1

A GET request can ask for a specific range. Range support is complete
except for multiple byte ranges.

#### DELETE

DELETE is similar to GET, except that a `204 No Content` response is
returned on success.
