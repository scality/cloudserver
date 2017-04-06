# Using S3 for continuous integration testing or in production with Docker

* [For continuous integration with Docker](#for-continuous-integration-with-docker)
  * [Environment Variables](#environment-variables)
* [In production with Docker](#in-production-with-docker)
  * [Using Docker Volume in production](#using-docker-volume-in-production)
  * [Adding modifying or deleting accounts or users credentials](#adding-modifying-or-deleting-accounts-or-users-credentials)
  * [Specifying your own host name](#specifying-your-own-host-name)
  * [Running as an unprivileged user](#running-as-an-unprivileged-user)

## For continuous integration with Docker

When you start the Docker Scality S3 server image, you can adjust the
configuration of the Scality S3 server instance by passing one or more
environment variables on the docker run command line.

### Environment Variables

#### HOST_NAME

This variable specifies a host name.
If you have a domain such as new.host.com, by specifying that here,
you and your users can direct s3 server requests to new.host.com.

```shell
docker run -d --name s3server -p 8000:8000 -e HOST_NAME=new.host.com scality/s3server
```

Note: In your `/etc/hosts` file on Linux, OS X, or Unix with root permissions,
make sure to associate 127.0.0.1 with `new.host.com`

#### SCALITY_ACCESS_KEY_ID and SCALITY_SECRET_ACCESS_KEY

These variables specify authentication credentials for an account
named "CustomAccount".

You can set credentials for many accounts by editing `conf/authdata.json`
(see below for further info),
but if you just want to specify one set of your own,
you can use these environment variables.

```shell
docker run -d --name s3server -p 8000:8000 -e SCALITY_ACCESS_KEY_ID=newAccessKey
-e SCALITY_SECRET_ACCESS_KEY=newSecretKey scality/s3server
```

Note: Anything in the `authdata.json` file will be ignored.
Note: The old `ACCESS_KEY` and `SECRET_KEY` environment variables are now
deprecated

#### LOG_LEVEL

This variable allows you to change the log level: info, debug or trace.
The default is info. Debug will give you more detailed logs and trace
will give you the most detailed.

```shell
docker run -d --name s3server -p 8000:8000 -e LOG_LEVEL=trace scality/s3server
```

#### SSL

This variable specifies the Common Name `<DOMAIN_NAME>` used to create the
Certificate Signing Request using OpenSSL. This allows you to run S3 with SSL:

**Note**:
In your `/etc/hosts` file on Linux, OS X, or Unix with root permissions,
make sure to associate 127.0.0.1 with `<SUBDOMAIN>.<DOMAIN_NAME>`

**Warning**:
These certs, being self-signed (and the CA being generated inside the container)
will be untrusted by any clients, and could disappear on a container upgrade.
That's ok as long as it's for quick testing.
Also, best security practice for non-testing would be to use an extra container
to do SSL/TLS termination such as haproxy/nginx/stunnel to limit what an exploit
on either component could expose, as well as certificates in a mounted volume

```shell
docker run -d --name s3server -p 8000:8000 -e SSL=<DOMAIN_NAME> -e HOST_NAME=<SUBDOMAIN>.<DOMAIN_NAME>
scality/s3server
```

More information about how to use S3 server with SSL
[here](https://s3.scality.com/v1.0/page/scality-with-ssl)

## In production with Docker

### Using Docker Volume in production

S3 server runs with a file backend by default.

So, by default, the data is stored inside your S3 server Docker container.

However, if you want your data and metadata to persist, you **MUST** use Docker
volumes to host your data and metadata outside your s3 server Docker container.
Otherwise, the data and metadata will be destroyed when you erase the container.

```shell
docker run -­v $(pwd)/data:/usr/src/app/localData -­v $(pwd)/metadata:/usr/src/app/localMetadata
-p 8000:8000 ­-d scality/s3server
```

This command mounts the host directory, `./data`, into the container at
/usr/src/app/localData and the host directory, `./metadata`, into the container
at /usr/src/app/localMetaData. It can also be any host mount point,
like `/mnt/data` and `/mnt/metadata`.

### Adding modifying or deleting accounts or users credentials

1. Create locally a customized `authdata.json`.

2. Use [Docker Volume](https://docs.docker.com/engine/tutorials/dockervolumes/)

to override the default `authdata.json` through a docker file mapping.
For example:

```shell
docker run -v $(pwd)/authdata.json:/usr/src/app/conf/authdata.json -p 8000:8000 -d
scality/s3server
```

### Specifying your own host name

To specify a host name (e.g. s3.domain.name),
you can provide your own
[config.json](https://github.com/scality/S3/blob/master/config.json)
using [Docker Volume](https://docs.docker.com/engine/tutorials/dockervolumes/).

First add a new key-value pair in the restEndpoints section of your config.json.
The key in the key-value pair should be the host name you would like to add
and the value is the default location_constraint for this endpoint.

For example, `s3.example.com` is mapped to `us-east-1` which is one of
the `location_constraints` listed in your locationConfig.json file
[here](https://github.com/scality/S3/blob/master/locationConfig.json).

More information about location configuration
[here](https://github.com/scality/S3/blob/master/README.md#location-configuration)

```json
"restEndpoints": {
    "localhost": "file",
    "127.0.0.1": "file",
    ...
    "s3.example.com": "us-east-1"
},
```

Then, run your Scality S3 Server using
[Docker Volume](https://docs.docker.com/engine/tutorials/dockervolumes/):

```shell
docker run -v $(pwd)/config.json:/usr/src/app/config.json -p 8000:8000 -d scality/s3server
```

Your local `config.json` file will override the default one through a docker
file mapping.

### Running as an unprivileged user

S3 Server runs as root by default.

You can change that by modifing the dockerfile and specifying a user before the entrypoint.

The user needs to exist within the container,
and own the folder **/usr/src/app** for Scality S3 Server to run properly.

For instance, you can modify these lines in the dockerfile:

```shell
...
&& groupadd -r -g 1001 scality \
&& useradd -u 1001 -g 1001 -d /usr/src/app -r scality \
&& chown -R scality:scality /usr/src/app

...

USER scality
ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]
```
