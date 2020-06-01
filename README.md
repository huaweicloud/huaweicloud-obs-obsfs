obsfs
====

obsfs is modified based on open source [s3fs](https://github.com/s3fs-fuse/s3fs-fuse).Obsfs inherits some functions of s3fs and develops some unique functions for Huawei cloud [OBS](https://www.huaweicloud.com/product/obs.html) services.obsfs allows Linux and Mac OS X to mount an S3 bucket via FUSE.

Features
--------

* large subset of POSIX including reading/writing files, directories, symlinks, mode, uid/gid, and extended attributes
* large files via multi-part upload
* renames via server-side copy
* data integrity via MD5 hashes
* in-memory metadata caching
* authenticate via v2 or v4 signatures

Installation
------------

* On Linux, ensure you have all the dependencies:

On Ubuntu 14.04:

```
sudo apt-get install automake autotools-dev fuse g++ git libcurl4-gnutls-dev libfuse-dev libssl-dev libxml2-dev make pkg-config
```

On CentOS 7:

```
sudo yum install automake fuse fuse-devel gcc-c++ git libcurl-devel libxml2-devel make openssl-devel
```

Then compile from master via the following commands:

```
git clone https://github.com/huaweicloud/huaweicloud-obs-obsfs.git
cd huaweicloud-obs-obsfs
./autogen.sh
./configure
make
sudo make install
```


Examples
--------

Enter your OBS identity and credential in a file `/path/to/passwd` and set
owner-only permissions:

```
echo AK:SK > /path/to/passwd
chmod 600 /path/to/passwd
```

Run obsfs with an existing bucket `mybucket` and directory `/path/to/mountpoint`:

```
obsfs mybucket /path/to/mountpoint -o passwd_file=/path/to/passwd -o use_ino
```

If you encounter any errors, enable debug output:

```
obsfs mybucket /path/to/mountpoint -o passwd_file=/path/to/passwd -o use_ino -o dbglevel=info -f -o curldbg
```


License
-------

Licensed under the GNU GPL version 2

