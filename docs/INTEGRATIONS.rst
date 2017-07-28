Integrations
++++++++++++

High Availability
=================

`Docker swarm <https://docs.docker.com/engine/swarm/>`__ is a
clustering tool developped by Docker and ready to use with its
containers. It allows to start a service, which we define and use as a
means to ensure Zenko CloudServer's continuous availability to the end user.
Indeed, a swarm defines a manager and n workers among n+1 servers. We
will do a basic setup in this tutorial, with just 3 servers, which
already provides a strong service resiliency, whilst remaining easy to
do as an individual. We will use NFS through docker to share data and
metadata between the different servers.

You will see that the steps of this tutorial are defined as **On
Server**, **On Clients**, **On All Machines**. This refers respectively
to NFS Server, NFS Clients, or NFS Server and Clients. In our example,
the IP of the Server will be **10.200.15.113**, while the IPs of the
Clients will be **10.200.15.96 and 10.200.15.97**

Installing docker
-----------------

Any version from docker 1.12.6 onwards should work; we used Docker
17.03.0-ce for this tutorial.

On All Machines
~~~~~~~~~~~~~~~

On Ubuntu 14.04
^^^^^^^^^^^^^^^

The docker website has `solid
documentation <https://docs.docker.com/engine/installation/linux/ubuntu/>`__.
We have chosen to install the aufs dependency, as recommended by Docker.
Here are the required commands:

.. code:: sh

    $> sudo apt-get update
    $> sudo apt-get install linux-image-extra-$(uname -r) linux-image-extra-virtual
    $> sudo apt-get install apt-transport-https ca-certificates curl software-properties-common
    $> curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
    $> sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
    $> sudo apt-get update
    $> sudo apt-get install docker-ce

On CentOS 7
^^^^^^^^^^^

The docker website has `solid
documentation <https://docs.docker.com/engine/installation/linux/centos/>`__.
Here are the required commands:

.. code:: sh

    $> sudo yum install -y yum-utils
    $> sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
    $> sudo yum makecache fast
    $> sudo yum install docker-ce
    $> sudo systemctl start docker

Configure NFS
-------------

On Clients
~~~~~~~~~~

Your NFS Clients will mount Docker volumes over your NFS Server's shared
folders. Hence, you don't have to mount anything manually, you just have
to install the NFS commons:

On Ubuntu 14.04
^^^^^^^^^^^^^^^

Simply install the NFS commons:

.. code:: sh

    $> sudo apt-get install nfs-common

On CentOS 7
^^^^^^^^^^^

Install the NFS utils, and then start the required services:

.. code:: sh

    $> yum install nfs-utils
    $> sudo systemctl enable rpcbind
    $> sudo systemctl enable nfs-server
    $> sudo systemctl enable nfs-lock
    $> sudo systemctl enable nfs-idmap
    $> sudo systemctl start rpcbind
    $> sudo systemctl start nfs-server
    $> sudo systemctl start nfs-lock
    $> sudo systemctl start nfs-idmap

On Server
~~~~~~~~~

Your NFS Server will be the machine to physically host the data and
metadata. The package(s) we will install on it is slightly different
from the one we installed on the clients.

On Ubuntu 14.04
^^^^^^^^^^^^^^^

Install the NFS server specific package and the NFS commons:

.. code:: sh

    $> sudo apt-get install nfs-kernel-server nfs-common

On CentOS 7
^^^^^^^^^^^

Same steps as with the client: install the NFS utils and start the
required services:

.. code:: sh

    $> yum install nfs-utils
    $> sudo systemctl enable rpcbind
    $> sudo systemctl enable nfs-server
    $> sudo systemctl enable nfs-lock
    $> sudo systemctl enable nfs-idmap
    $> sudo systemctl start rpcbind
    $> sudo systemctl start nfs-server
    $> sudo systemctl start nfs-lock
    $> sudo systemctl start nfs-idmap

On Ubuntu 14.04 and CentOS 7
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Choose where your shared data and metadata from your local `Zenko CloudServer
<http://www.zenko.io/cloudserver/>`__ will be stored.
We chose to go with /var/nfs/data and /var/nfs/metadata. You also need
to set proper sharing permissions for these folders as they'll be shared
over NFS:

.. code:: sh

    $> mkdir -p /var/nfs/data /var/nfs/metadata
    $> chmod -R 777 /var/nfs/

Now you need to update your **/etc/exports** file. This is the file that
configures network permissions and rwx permissions for NFS access. By
default, Ubuntu applies the no\_subtree\_check option, so we declared
both folders with the same permissions, even though they're in the same
tree:

.. code:: sh

    $> sudo vim /etc/exports

In this file, add the following lines:

.. code:: sh

    /var/nfs/data        10.200.15.96(rw,sync,no_root_squash) 10.200.15.97(rw,sync,no_root_squash)
    /var/nfs/metadata    10.200.15.96(rw,sync,no_root_squash) 10.200.15.97(rw,sync,no_root_squash)

Export this new NFS table:

.. code:: sh

    $> sudo exportfs -a

Eventually, you need to allow for NFS mount from Docker volumes on other
machines. You need to change the Docker config in
**/lib/systemd/system/docker.service**:

.. code:: sh

    $> sudo vim /lib/systemd/system/docker.service

In this file, change the **MountFlags** option:

.. code:: sh

    MountFlags=shared

Now you just need to restart the NFS server and docker daemons so your
changes apply.

On Ubuntu 14.04
^^^^^^^^^^^^^^^

Restart your NFS Server and docker services:

.. code:: sh

    $> sudo service nfs-kernel-server restart
    $> sudo service docker restart

On CentOS 7
^^^^^^^^^^^

Restart your NFS Server and docker daemons:

.. code:: sh

    $> sudo systemctl restart nfs-server
    $> sudo systemctl daemon-reload
    $> sudo systemctl restart docker

Set up your Docker Swarm service
--------------------------------

On All Machines
~~~~~~~~~~~~~~~

On Ubuntu 14.04 and CentOS 7
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We will now set up the Docker volumes that will be mounted to the NFS
Server and serve as data and metadata storage for Zenko CloudServer. These two
commands have to be replicated on all machines:

.. code:: sh

    $> docker volume create --driver local --opt type=nfs --opt o=addr=10.200.15.113,rw --opt device=:/var/nfs/data --name data
    $> docker volume create --driver local --opt type=nfs --opt o=addr=10.200.15.113,rw --opt device=:/var/nfs/metadata --name metadata

There is no need to ""docker exec" these volumes to mount them: the
Docker Swarm manager will do it when the Docker service will be started.

On Server
^^^^^^^^^

To start a Docker service on a Docker Swarm cluster, you first have to
initialize that cluster (i.e.: define a manager), then have the
workers/nodes join in, and then start the service. Initialize the swarm
cluster, and look at the response:

.. code:: sh

    $> docker swarm init --advertise-addr 10.200.15.113

    Swarm initialized: current node (db2aqfu3bzfzzs9b1kfeaglmq) is now a manager.

    To add a worker to this swarm, run the following command:

        docker swarm join \
        --token SWMTKN-1-5yxxencrdoelr7mpltljn325uz4v6fe1gojl14lzceij3nujzu-2vfs9u6ipgcq35r90xws3stka \
        10.200.15.113:2377

    To add a manager to this swarm, run 'docker swarm join-token manager' and follow the instructions.

On Clients
^^^^^^^^^^

Simply copy/paste the command provided by your docker swarm init. When
all goes well, you'll get something like this:

.. code:: sh

    $> docker swarm join --token SWMTKN-1-5yxxencrdoelr7mpltljn325uz4v6fe1gojl14lzceij3nujzu-2vfs9u6ipgcq35r90xws3stka 10.200.15.113:2377

    This node joined a swarm as a worker.

On Server
^^^^^^^^^

Start the service on your swarm cluster!

.. code:: sh

    $> docker service create --name s3 --replicas 1 --mount type=volume,source=data,target=/usr/src/app/localData --mount type=volume,source=metadata,target=/usr/src/app/localMetadata -p 8000:8000 scality/s3server

If you run a docker service ls, you should have the following output:

.. code:: sh

    $> docker service ls
    ID            NAME  MODE        REPLICAS  IMAGE
    ocmggza412ft  s3    replicated  1/1       scality/s3server:latest

If your service won't start, consider disabling apparmor/SELinux.

Testing your High Availability S3Server
---------------------------------------

On All Machines
~~~~~~~~~~~~~~~

On Ubuntu 14.04 and CentOS 7
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Try to find out where your Scality Zenko CloudServer is actually running using
the **docker ps** command. It can be on any node of the swarm cluster,
manager or worker. When you find it, you can kill it, with **docker stop
<container id>** and you'll see it respawn on a different node of the
swarm cluster. Now you see, if one of your servers falls, or if docker
stops unexpectedly, your end user will still be able to access your
local Zenko CloudServer.

Troubleshooting
---------------

To troubleshoot the service you can run:

.. code:: sh

    $> docker service ps s3docker service ps s3
    ID                         NAME      IMAGE             NODE                               DESIRED STATE  CURRENT STATE       ERROR
    0ar81cw4lvv8chafm8pw48wbc  s3.1      scality/s3server  localhost.localdomain.localdomain  Running        Running 7 days ago
    cvmf3j3bz8w6r4h0lf3pxo6eu   \_ s3.1  scality/s3server  localhost.localdomain.localdomain  Shutdown       Failed 7 days ago   "task: non-zero exit (137)"

If the error is truncated it is possible to have a more detailed view of
the error by inspecting the docker task ID:

.. code:: sh

    $> docker inspect cvmf3j3bz8w6r4h0lf3pxo6eu

Off you go!
-----------

Let us know what you use this functionality for, and if you'd like any
specific developments around it. Or, even better: come and contribute to
our `Github repository <https://github.com/scality/s3/>`__! We look
forward to meeting you!


S3FS
====
Export your buckets as a filesystem with s3fs on top of Zenko CloudServer

`s3fs <https://github.com/s3fs-fuse/s3fs-fuse>`__ is an open source
tool that allows you to mount an S3 bucket on a filesystem-like backend.
It is available both on Debian and RedHat distributions. For this
tutorial, we used an Ubuntu 14.04 host to deploy and use s3fs over
Scality's Zenko CloudServer.

Deploying Zenko CloudServer with SSL
----------------------------

First, you need to deploy **Zenko CloudServer**. This can be done very easily
via `our DockerHub
page <https://hub.docker.com/r/scality/s3server/>`__ (you want to run it
with a file backend).

    *Note:* *- If you don't have docker installed on your machine, here
    are the `instructions to install it for your
    distribution <https://docs.docker.com/engine/installation/>`__*

You also necessarily have to set up SSL with Zenko CloudServer to use s3fs. We
have a nice
`tutorial <https://s3.scality.com/v1.0/page/scality-with-ssl>`__ to help
you do it.

s3fs setup
----------

Installing s3fs
~~~~~~~~~~~~~~~

s3fs has quite a few dependencies. As explained in their
`README <https://github.com/s3fs-fuse/s3fs-fuse/blob/master/README.md#installation>`__,
the following commands should install everything for Ubuntu 14.04:

.. code:: sh

    $> sudo apt-get install automake autotools-dev g++ git libcurl4-gnutls-dev
    $> sudo apt-get install libfuse-dev libssl-dev libxml2-dev make pkg-config

Now you want to install s3fs per se:

.. code:: sh

    $> git clone https://github.com/s3fs-fuse/s3fs-fuse.git
    $> cd s3fs-fuse
    $> ./autogen.sh
    $> ./configure
    $> make
    $> sudo make install

Check that s3fs is properly installed by checking its version. it should
answer as below:

.. code:: sh

     $> s3fs --version

    Amazon Simple Storage Service File System V1.80(commit:d40da2c) with OpenSSL

Configuring s3fs
~~~~~~~~~~~~~~~~

s3fs expects you to provide it with a password file. Our file is
``/etc/passwd-s3fs``. The structure for this file is
``ACCESSKEYID:SECRETKEYID``, so, for S3Server, you can run:

.. code:: sh

    $> echo 'accessKey1:verySecretKey1' > /etc/passwd-s3fs
    $> chmod 600 /etc/passwd-s3fs

Using Zenko CloudServer with s3fs
------------------------

First, you're going to need a mountpoint; we chose ``/mnt/tests3fs``:

.. code:: sh

    $> mkdir /mnt/tests3fs

Then, you want to create a bucket on your local Zenko CloudServer; we named it
``tests3fs``:

.. code:: sh

    $> s3cmd mb s3://tests3fs

    *Note:* *- If you've never used s3cmd with our Zenko CloudServer, our README
    provides you with a `recommended
    config <https://github.com/scality/S3/blob/master/README.md#s3cmd>`__*

Now you can mount your bucket to your mountpoint with s3fs:

.. code:: sh

    $> s3fs tests3fs /mnt/tests3fs -o passwd_file=/etc/passwd-s3fs -o url="https://s3.scality.test:8000/" -o use_path_request_style

    *If you're curious, the structure of this command is*
    ``s3fs BUCKET_NAME PATH/TO/MOUNTPOINT -o OPTIONS``\ *, and the
    options are mandatory and serve the following purposes:
    * ``passwd_file``\ *: specifiy path to password file;
    * ``url``\ *: specify the hostname used by your SSL provider;
    * ``use_path_request_style``\ *: force path style (by default, s3fs
    uses subdomains (DNS style)).*

| From now on, you can either add files to your mountpoint, or add
  objects to your bucket, and they'll show in the other.
| For example, let's' create two files, and then a directory with a file
  in our mountpoint:

.. code:: sh

    $> touch /mnt/tests3fs/file1 /mnt/tests3fs/file2
    $> mkdir /mnt/tests3fs/dir1
    $> touch /mnt/tests3fs/dir1/file3

Now, I can use s3cmd to show me what is actually in S3Server:

.. code:: sh

    $> s3cmd ls -r s3://tests3fs

    2017-02-28 17:28         0   s3://tests3fs/dir1/
    2017-02-28 17:29         0   s3://tests3fs/dir1/file3
    2017-02-28 17:28         0   s3://tests3fs/file1
    2017-02-28 17:28         0   s3://tests3fs/file2

Now you can enjoy a filesystem view on your local Zenko CloudServer!


Duplicity
=========

How to backup your files with Zenko CloudServer.

Installing
-----------

Installing Duplicity and its dependencies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Second, you want to install
`Duplicity <http://duplicity.nongnu.org/index.html>`__. You have to
download `this
tarball <https://code.launchpad.net/duplicity/0.7-series/0.7.11/+download/duplicity-0.7.11.tar.gz>`__,
decompress it, and then checkout the README inside, which will give you
a list of dependencies to install. If you're using Ubuntu 14.04, this is
your lucky day: here is a lazy step by step install.

.. code:: sh

    $> apt-get install librsync-dev gnupg
    $> apt-get install python-dev python-pip python-lockfile
    $> pip install -U boto

Then you want to actually install Duplicity:

.. code:: sh

    $> tar zxvf duplicity-0.7.11.tar.gz
    $> cd duplicity-0.7.11
    $> python setup.py install

Using
------

Testing your installation
~~~~~~~~~~~~~~~~~~~~~~~~~~~

First, we're just going to quickly check that Zenko CloudServer is actually
running. To do so, simply run ``$> docker ps`` . You should see one
container named ``scality/s3server``. If that is not the case, try
``$> docker start s3server``, and check again.

Secondly, as you probably know, Duplicity uses a module called **Boto**
to send requests to S3. Boto requires a configuration file located in
**``/etc/boto.cfg``** to have your credentials and preferences. Here is
a minimalistic config `that you can finetune following these
instructions <http://boto.cloudhackers.com/en/latest/getting_started.html>`__.

::

    [Credentials]
    aws_access_key_id = accessKey1
    aws_secret_access_key = verySecretKey1

    [Boto]
    # If using SSL, set to True
    is_secure = False
    # If using SSL, unmute and provide absolute path to local CA certificate
    # ca_certificates_file = /absolute/path/to/ca.crt

    *Note:* *If you want to set up SSL with Zenko CloudServer, check out our
    `tutorial <http://link/to/SSL/tutorial>`__*

At this point, we've met all the requirements to start running Zenko CloudServer
as a backend to Duplicity. So we should be able to back up a local
folder/file to local S3. Let's try with the duplicity decompressed
folder:

.. code:: sh

    $> duplicity duplicity-0.7.11 "s3://127.0.0.1:8000/testbucket/"

    *Note:* *Duplicity will prompt you for a symmetric encryption
    passphrase. Save it somewhere as you will need it to recover your
    data. Alternatively, you can also add the ``--no-encryption`` flag
    and the data will be stored plain.*

If this command is succesful, you will get an output looking like this:

::

    --------------[ Backup Statistics ]--------------
    StartTime 1486486547.13 (Tue Feb  7 16:55:47 2017)
    EndTime 1486486547.40 (Tue Feb  7 16:55:47 2017)
    ElapsedTime 0.27 (0.27 seconds)
    SourceFiles 388
    SourceFileSize 6634529 (6.33 MB)
    NewFiles 388
    NewFileSize 6634529 (6.33 MB)
    DeletedFiles 0
    ChangedFiles 0
    ChangedFileSize 0 (0 bytes)
    ChangedDeltaSize 0 (0 bytes)
    DeltaEntries 388
    RawDeltaSize 6392865 (6.10 MB)
    TotalDestinationSizeChange 2003677 (1.91 MB)
    Errors 0
    -------------------------------------------------

Congratulations! You can now backup to your local S3 through duplicity
:)

Automating backups
~~~~~~~~~~~~~~~~~~~

Now you probably want to back up your files periodically. The easiest
way to do this is to write a bash script and add it to your crontab.
Here is my suggestion for such a file:

.. code:: sh

    #!/bin/bash

    # Export your passphrase so you don't have to type anything
    export PASSPHRASE="mypassphrase"

    # If you want to use a GPG Key, put it here and unmute the line below
    #GPG_KEY=

    # Define your backup bucket, with localhost specified
    DEST="s3://127.0.0.1:8000/testbuckets3server/"

    # Define the absolute path to the folder you want to backup
    SOURCE=/root/testfolder

    # Set to "full" for full backups, and "incremental" for incremental backups
    # Warning: you have to perform one full backup befor you can perform
    # incremental ones on top of it
    FULL=incremental

    # How long to keep backups for; if you don't want to delete old
    # backups, keep empty; otherwise, syntax is "1Y" for one year, "1M"
    # for one month, "1D" for one day
    OLDER_THAN="1Y"

    # is_running checks whether duplicity is currently completing a task
    is_running=$(ps -ef | grep duplicity  | grep python | wc -l)

    # If duplicity is already completing a task, this will simply not run
    if [ $is_running -eq 0 ]; then
        echo "Backup for ${SOURCE} started"

        # If you want to delete backups older than a certain time, we do it here
        if [ "$OLDER_THAN" != "" ]; then
            echo "Removing backups older than ${OLDER_THAN}"
            duplicity remove-older-than ${OLDER_THAN} ${DEST}
        fi

        # This is where the actual backup takes place
        echo "Backing up ${SOURCE}..."
        duplicity ${FULL} \
            ${SOURCE} ${DEST}
            # If you're using GPG, paste this in the command above
            # --encrypt-key=${GPG_KEY} --sign-key=${GPG_KEY} \
            # If you want to exclude a subfolder/file, put it below and
            # paste this
            # in the command above
            # --exclude=/${SOURCE}/path_to_exclude \

        echo "Backup for ${SOURCE} complete"
        echo "------------------------------------"
    fi
    # Forget the passphrase...
    unset PASSPHRASE

So let's say you put this file in ``/usr/local/sbin/backup.sh.`` Next
you want to run ``crontab -e`` and paste your configuration in the file
that opens. If you're unfamiliar with Cron, here is a good `How
To <https://help.ubuntu.com/community/CronHowto>`__. The folder I'm
backing up is a folder I modify permanently during my workday, so I want
incremental backups every 5mn from 8AM to 9PM monday to friday. Here is
the line I will paste in my crontab:

.. code:: cron

    */5 8-20 * * 1-5 /usr/local/sbin/backup.sh

Now I can try and add / remove files from the folder I'm backing up, and
I will see incremental backups in my bucket.
