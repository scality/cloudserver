Integrations
++++++++++++

High Availability
=================

`Docker Swarm <https://docs.docker.com/engine/swarm/>`__ is a clustering tool
developed by Docker for use with its containers. It can be used to start
services, which we define to ensure CloudServer's continuous availability to
end users. A swarm defines a manager and *n* workers among *n* + 1 servers.

This tutorial shows how to perform a basic setup with three servers, which
provides strong service resiliency, while remaining easy to use and
maintain. We will use NFS through Docker to share data and
metadata between the different servers.

Sections are labeled **On Server**, **On Clients**, or
**On All Machines**, referring respectively to NFS server, NFS clients, or
NFS server and clients. In the present example, the server’s IP address is
**10.200.15.113** and the client IP addresses are **10.200.15.96** and
**10.200.15.97**

1. Install Docker (on All Machines)

   Docker 17.03.0-ce is used for this tutorial. Docker 1.12.6 and later will
   likely work, but is not tested.

   * On Ubuntu 14.04
     Install Docker CE for Ubuntu as `documented at Docker
     <https://docs.docker.com/install/linux/docker-ce/ubuntu/>`__.
     Install the aufs dependency as recommended by Docker. The required
     commands are:

     .. code:: sh

      $> sudo apt-get update
      $> sudo apt-get install linux-image-extra-$(uname -r) linux-image-extra-virtual
      $> sudo apt-get install apt-transport-https ca-certificates curl software-properties-common
      $> curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
      $> sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
      $> sudo apt-get update
      $> sudo apt-get install docker-ce

    * On CentOS 7
      Install Docker CE as `documented at Docker
      <https://docs.docker.com/install/linux/docker-ce/centos/>`__.
      The required commands are:

      .. code:: sh

        $> sudo yum install -y yum-utils
        $> sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
        $> sudo yum makecache fast
        $> sudo yum install docker-ce
        $> sudo systemctl start docker

2. Install NFS on Client(s)

   NFS clients mount Docker volumes over the NFS server’s shared folders.
   If the NFS commons are installed, manual mounts are no longer needed.

   * On Ubuntu 14.04

     Install the NFS commons with apt-get:

     .. code:: sh

       $> sudo apt-get install nfs-common

   * On CentOS 7

     Install the NFS utils; then start required services:

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

3. Install NFS (on Server)

   The NFS server hosts the data and metadata. The package(s) to install on it
   differs from the package installed on the clients.

   * On Ubuntu 14.04

     Install the NFS server-specific package and the NFS commons:

     .. code:: sh

      $> sudo apt-get install nfs-kernel-server nfs-common

   * On CentOS 7

     Install the NFS utils and start the required services:

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

   For both distributions:

   #. Choose where shared data and metadata from the local
      `CloudServer <http://www.zenko.io/cloudserver/>`__ shall be stored (The
      present example uses /var/nfs/data and /var/nfs/metadata). Set permissions
      for these folders for
      sharing over NFS:

      .. code:: sh

        $> mkdir -p /var/nfs/data /var/nfs/metadata
        $> chmod -R 777 /var/nfs/

   #. The /etc/exports file configures network permissions and r-w-x permissions
      for NFS access. Edit /etc/exports, adding the following lines:

      .. code:: sh

        /var/nfs/data        10.200.15.96(rw,sync,no_root_squash) 10.200.15.97(rw,sync,no_root_squash)
        /var/nfs/metadata    10.200.15.96(rw,sync,no_root_squash) 10.200.15.97(rw,sync,no_root_squash)

      Ubuntu applies the no\_subtree\_check option by default, so both
      folders are declared with the same permissions, even though they’re in
      the same tree.

   #. Export this new NFS table:

      .. code:: sh

        $> sudo exportfs -a

   #. Edit the ``MountFlags`` option in the Docker config in
      /lib/systemd/system/docker.service to enable NFS mount from Docker volumes
      on other machines:

      .. code:: sh

        MountFlags=shared

   #. Restart the NFS server and Docker daemons to apply these changes.

      * On Ubuntu 14.04

        .. code:: sh

          $> sudo service nfs-kernel-server restart
          $> sudo service docker restart

      * On CentOS 7

        .. code:: sh

         $> sudo systemctl restart nfs-server
         $> sudo systemctl daemon-reload
         $> sudo systemctl restart docker


4. Set Up a Docker Swarm

  * On all machines and distributions:

    Set up the Docker volumes to be mounted to the NFS server for CloudServer’s
    data and metadata storage. The following commands must be replicated on all
    machines:

    .. code:: sh

     $> docker volume create --driver local --opt type=nfs --opt o=addr=10.200.15.113,rw --opt device=:/var/nfs/data --name data
     $> docker volume create --driver local --opt type=nfs --opt o=addr=10.200.15.113,rw --opt device=:/var/nfs/metadata --name metadata

    There is no need to ``docker exec`` these volumes to mount them: the
    Docker Swarm manager does this when the Docker service is started.

  * On a server:

    To start a Docker service on a Docker Swarm cluster, initialize the cluster
    (that is, define a manager), prompt workers/nodes to join in, and then start
    the service.

    Initialize the swarm cluster, and review its response:

    .. code:: sh

      $> docker swarm init --advertise-addr 10.200.15.113

      Swarm initialized: current node (db2aqfu3bzfzzs9b1kfeaglmq) is now a manager.

      To add a worker to this swarm, run the following command:

      docker swarm join \
      --token SWMTKN-1-5yxxencrdoelr7mpltljn325uz4v6fe1gojl14lzceij3nujzu-2vfs9u6ipgcq35r90xws3stka \
      10.200.15.113:2377

      To add a manager to this swarm, run 'docker swarm join-token manager' and follow the instructions.

  * On clients:

    Copy and paste the command provided by your Docker Swarm init. A successful
    request/response will resemble:

    .. code:: sh

      $> docker swarm join --token SWMTKN-1-5yxxencrdoelr7mpltljn325uz4v6fe1gojl14lzceij3nujzu-2vfs9u6ipgcq35r90xws3stka 10.200.15.113:2377

      This node joined a swarm as a worker.

Set Up Docker Swarm on Clients on a Server
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Start the service on the Swarm cluster.

.. code:: sh

  $> docker service create --name s3 --replicas 1 --mount type=volume,source=data,target=/usr/src/app/localData --mount type=volume,source=metadata,target=/usr/src/app/localMetadata -p 8000:8000 scality/cloudserver

On a successful installation, ``docker service ls`` returns the following
output:

.. code:: sh

    $> docker service ls
    ID            NAME  MODE        REPLICAS  IMAGE
    ocmggza412ft  s3    replicated  1/1       scality/cloudserver:latest

If the service does not start, consider disabling apparmor/SELinux.

Testing the High-Availability CloudServer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

On all machines (client/server) and distributions (Ubuntu and CentOS),
determine where CloudServer is running using ``docker ps``. CloudServer can
operate on any node of the Swarm cluster, manager or worker. When you find
it, you can kill it with ``docker stop <container id>``. It will respawn
on a different node. Now, if one server falls, or if Docker stops
unexpectedly, the end user will still be able to access your the local CloudServer.

Troubleshooting
~~~~~~~~~~~~~~~

To troubleshoot the service, run:

.. code:: sh

    $> docker service ps s3docker service ps s3
    ID                         NAME      IMAGE             NODE                               DESIRED STATE  CURRENT STATE       ERROR
    0ar81cw4lvv8chafm8pw48wbc  s3.1      scality/cloudserver  localhost.localdomain.localdomain  Running        Running 7 days ago
    cvmf3j3bz8w6r4h0lf3pxo6eu   \_ s3.1  scality/cloudserver  localhost.localdomain.localdomain  Shutdown       Failed 7 days ago   "task: non-zero exit (137)"

If the error is truncated, view the error in detail by inspecting the
Docker task ID:

.. code:: sh

    $> docker inspect cvmf3j3bz8w6r4h0lf3pxo6eu

Off you go!
~~~~~~~~~~~

Let us know how you use this and if you'd like any specific developments 
around it. Even better: come and contribute to our `Github repository 
<https://github.com/scality/s3/>`__! We look forward to meeting you!

S3FS
====

You can export buckets as a filesystem with s3fs on CloudServer.

`s3fs <https://github.com/s3fs-fuse/s3fs-fuse>`__ is an open source
tool, available both on Debian and RedHat distributions, that enables
you to mount an S3 bucket on a filesystem-like backend. This tutorial uses
an Ubuntu 14.04 host to deploy and use s3fs over CloudServer.

Deploying Zenko CloudServer with SSL
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

First, deploy CloudServer with a file backend using `our DockerHub page
<https://hub.docker.com/r/zenko/cloudserver>`__.

.. note::

  If Docker is not installed on your machine, follow
  `these instructions <https://docs.docker.com/engine/installation/>`__
  to install it for your distribution.

You must also set up SSL with CloudServer to use s3fs. See `Using SSL
<./GETTING_STARTED#Using_SSL>`__ for instructions.

s3fs Setup
~~~~~~~~~~

Installing s3fs
---------------

Follow the instructions in the s3fs `README
<https://github.com/s3fs-fuse/s3fs-fuse/blob/master/README.md#installation-from-pre-built-packages>`__,

Check that s3fs is properly installed. A version check should return
a response resembling:

.. code:: sh

    $> s3fs --version

    Amazon Simple Storage Service File System V1.80(commit:d40da2c) with OpenSSL
    Copyright (C) 2010 Randy Rizun <rrizun@gmail.com>
    License GPL2: GNU GPL version 2 <http://gnu.org/licenses/gpl.html>
    This is free software: you are free to change and redistribute it.
    There is NO WARRANTY, to the extent permitted by law.

Configuring s3fs
----------------

s3fs expects you to provide it with a password file. Our file is
``/etc/passwd-s3fs``. The structure for this file is
``ACCESSKEYID:SECRETKEYID``, so, for CloudServer, you can run:

.. code:: sh

    $> echo 'accessKey1:verySecretKey1' > /etc/passwd-s3fs
    $> chmod 600 /etc/passwd-s3fs

Using CloudServer with s3fs
---------------------------

1. Use /mnt/tests3fs as a mount point.

   .. code:: sh

    $> mkdir /mnt/tests3fs

2. Create a bucket on your local CloudServer. In the present example it is
   named “tests3fs”.

   .. code:: sh

    $> s3cmd mb s3://tests3fs

3. Mount the bucket to your mount point with s3fs:

   .. code:: sh

    $> s3fs tests3fs /mnt/tests3fs -o passwd_file=/etc/passwd-s3fs -o url="https://s3.scality.test:8000/" -o use_path_request_style

   The structure of this command is:
   ``s3fs BUCKET_NAME PATH/TO/MOUNTPOINT -o OPTIONS``. Of these mandatory
   options:

   * ``passwd_file`` specifies the path to the password file.
   * ``url`` specifies the host name used by your SSL provider.
   * ``use_path_request_style`` forces the path style (by default,
       s3fs uses DNS-style subdomains).

Once the bucket is mounted, files added to the mount point or
objects added to the bucket will appear in both locations.

Example
-------

   Create two files, and then a directory with a file in our mount point:

   .. code:: sh

      $> touch /mnt/tests3fs/file1 /mnt/tests3fs/file2
      $> mkdir /mnt/tests3fs/dir1
      $> touch /mnt/tests3fs/dir1/file3

   Now, use s3cmd to show what is in CloudServer:

   .. code:: sh

      $> s3cmd ls -r s3://tests3fs

      2017-02-28 17:28         0   s3://tests3fs/dir1/
      2017-02-28 17:29         0   s3://tests3fs/dir1/file3
      2017-02-28 17:28         0   s3://tests3fs/file1
      2017-02-28 17:28         0   s3://tests3fs/file2

   Now you can enjoy a filesystem view on your local CloudServer.


Duplicity
=========

How to back up your files with CloudServer.

Installing Duplicity and its Dependencies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To install `Duplicity <http://duplicity.nongnu.org/>`__,
go to `this site <https://code.launchpad.net/duplicity/0.7-series>`__.
Download the latest tarball. Decompress it and follow the instructions
in the README.

.. code:: sh

   $> tar zxvf duplicity-0.7.11.tar.gz
   $> cd duplicity-0.7.11
   $> python setup.py install

You may receive error messages indicating the need to install some or all
of the following dependencies:

.. code:: sh

    $> apt-get install librsync-dev gnupg
    $> apt-get install python-dev python-pip python-lockfile
    $> pip install -U boto

Testing the Installation
------------------------

1. Check that CloudServer is running. Run ``$> docker ps``. You should
   see one container named ``scality/cloudserver``. If you do not, run
   ``$> docker start cloudserver`` and check again.


2. Duplicity uses a module called “Boto” to send requests to S3. Boto
   requires a configuration file located in ``/etc/boto.cfg`` to store
   your credentials and preferences. A minimal configuration
   you can fine tune `following these instructions
   <http://boto.cloudhackers.com/en/latest/getting_started.html>`__ is
   shown here:

::

    [Credentials]
    aws_access_key_id = accessKey1
    aws_secret_access_key = verySecretKey1

    [Boto]
    # If using SSL, set to True
    is_secure = False
    # If using SSL, unmute and provide absolute path to local CA certificate
    # ca_certificates_file = /absolute/path/to/ca.crt

 .. note:: To set up SSL with CloudServer, check out our `Using SSL
	   <./GETTING_STARTED#Using_SSL>`__ in GETTING STARTED.

3. At this point all requirements to run CloudServer as a backend to Duplicity
   have been met. A local folder/file should back up to the local S3.
   Try it with the decompressed Duplicity folder:

.. code:: sh

    $> duplicity duplicity-0.7.11 "s3://127.0.0.1:8000/testbucket/"

.. note:: Duplicity will prompt for a symmetric encryption passphrase.
	  Save it carefully, as you will need it to recover your data.
	  Alternatively, you can add the ``--no-encryption`` flag
	  and the data will be stored plain.

   If this command is successful, you will receive an output resembling:

   .. code:: sh

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

Congratulations! You can now back up to your local S3 through Duplicity.

Automating Backups
------------------

The easiest way to back up files periodically is to write a bash script
and add it to your crontab. A suggested script follows.

.. code:: sh

    #!/bin/bash

    # Export your passphrase so you don't have to type anything
    export PASSPHRASE="mypassphrase"

    # To use a GPG key, put it here and uncomment the line below
    #GPG_KEY=

    # Define your backup bucket, with localhost specified
    DEST="s3://127.0.0.1:8000/testbucketcloudserver/"

    # Define the absolute path to the folder to back up
    SOURCE=/root/testfolder

    # Set to "full" for full backups, and "incremental" for incremental backups
    # Warning: you must perform one full backup befor you can perform
    # incremental ones on top of it
    FULL=incremental

    # How long to keep backups. If you don't want to delete old backups, keep
    # this value empty; otherwise, the syntax is "1Y" for one year, "1M" for
    # one month, "1D" for one day.
    OLDER_THAN="1Y"

    # is_running checks whether Duplicity is currently completing a task
    is_running=$(ps -ef | grep duplicity  | grep python | wc -l)

    # If Duplicity is already completing a task, this will not run
    if [ $is_running -eq 0 ]; then
        echo "Backup for ${SOURCE} started"

        # To delete backups older than a certain time, do it here
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

Put this file in ``/usr/local/sbin/backup.sh``. Run ``crontab -e`` and
paste your configuration into the file that opens. If you're unfamiliar
with Cron, here is a good `HowTo
<https://help.ubuntu.com/community/CronHowto>`__. If the folder being
backed up is a folder to be modified permanently during the work day,
we can set incremental backups every 5 minutes from 8 AM to 9 PM Monday
through Friday by pasting the following line into crontab:

.. code:: sh

    */5 8-20 * * 1-5 /usr/local/sbin/backup.sh

Adding or removing files from the folder being backed up will result in
incremental backups in the bucket.
