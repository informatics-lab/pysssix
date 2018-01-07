# pysssix

Mount S3 as a POSIX like file system. Access is read only and assumes data on S3 is static.

Set up AWS credentials as per http://boto3.readthedocs.io/en/latest/guide/configuration.html using a method that doesn't require accessing the boto `Session` or `boto.client` object/methods.

```
pysssix <mount_point> [--debug]
```

`<mount_point>` should be a existing empty directory.

Further options:

```
usage: -c [-h] [-v] [-a] [-p PORT] [-c CACHE_SIZE] [-b BLOCK_SIZE]
          [-l CACHE_LOCATION]
          mount_point

positional arguments:
  mount_point           where to mount S3

optional arguments:
  -h, --help            show this help message and exit
  -v, --verbose         increase output verbosity
  -a, --allow_other     pass allow_other=True to FUSE
  -p PORT, --port PORT  port to use for client server communication
  -c CACHE_SIZE, --cache_size CACHE_SIZE
                        Approximate size of the cache in bytes
  -b BLOCK_SIZE, --block_size BLOCK_SIZE
                        Block size for requests
  -l CACHE_LOCATION, --cache_location CACHE_LOCATION
                        Path to disk location to store cache
```


Requires `libfuse` to be installed on the system. 
 
Ubuntu:
```
sudo apt-get install libfuse-dev
``` 

 
Work in progress use at own risk :)



 # Developer notes, set up and running in container.

 - build: 
 ```
 docker build  -t pysssix .
 ```

 - run:
 ```
 docker run -i -t --cap-add SYS_ADMIN --device /dev/fuse  -p 7766:7766 -v ~/.aws:/root/.aws -v $(pwd):/root/pysssix pysssix
 ```

## Using
```
cd /root
mkdir s3
pip install --upgrade --no-deps --force-reinstall  ./pysssix/
pysssix s3 -v
```

In another process

```
cat /root/s3/<my bucket>/<my key>
```

Run jupyter if required
```
cd ~
jupyter-notebook --allow-root --port 7766  --ip=0.0.0.0 
```


Find orphaned fuse process...
```

ubuntu@ip-172-31-19-222:~$ fuser /dev/fuse
/dev/fuse:            6671
ubuntu@ip-172-31-19-222:~$ kill 6671
````