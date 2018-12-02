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

Enter your S3 identity and credential in a file `/path/to/passwd` and set
owner-only permissions:

```
echo MYIDENTITY:MYCREDENTIAL > /path/to/passwd
chmod 600 /path/to/passwd
```

Run obsfs with an existing bucket `mybucket` and directory `/path/to/mountpoint`:

```
obsfs mybucket /path/to/mountpoint -o passwd_file=/path/to/passwd
```

If you encounter any errors, enable debug output:

```
obsfs mybucket /path/to/mountpoint -o passwd_file=/path/to/passwd -o dbglevel=info -f -o curldbg
```

If you use obsfs with a non-Amazon S3 implementation, specify the URL and path-style requests:

```
obsfs mybucket /path/to/mountpoint -o passwd_file=/path/to/passwd -o url=http://url.to.s3/ -o use_path_request_style
```

or(fstab)
```
obsfs#mybucket /path/to/mountpoint fuse _netdev,allow_other,use_path_request_style,url=http://url.to.s3/ 0 0
```

To use IBM IAM Authentication, use the `-o ibm_iam_auth` option, and specify the Service Instance ID and API Key in your credentials file:
```
echo SERVICEINSTANCEID:APIKEY > /path/to/passwd
```
The Service Instance ID is only required when using the `-o create_bucket` option.

Note: You may also want to create the global credential file first

```
echo MYIDENTITY:MYCREDENTIAL > /etc/passwd-obsfs
chmod 600 /etc/passwd-obsfs
```

Note2: You may also need to make sure `netfs` service is start on boot



License
-------

Licensed under the GNU GPL version 2

