# pysssix

Mount S3 as a POSIX like file system. Access is read only and assumes data on S3 is static.

Set up AWS credentials as per http://boto3.readthedocs.io/en/latest/guide/configuration.html using a method that doesn't require accessing the boto `Session` or `boto.client` object/methods.

```
pysssix <mount_point> [--debug]
```

`<mount_point>` should be a existing empty directory.

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
 docker run -i -t --cap-add SYS_ADMIN --device /dev/fuse  -v ~/.aws:/root/.aws -v $(pwd):/root/pysssix pysssix
 ```

## Using
```
cd /root
mkdir s3
pip install --upgrade --no-deps --force-reinstall  ./pysssix/
pysssix s3 --debug
```

In another process

```
cat /root/s3/<my bucket>/<my key>
```