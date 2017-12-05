
 - build: 
 ```
 docker build  -t s3fuse .
 ```

 - run:
 ```
 docker run -i -t --cap-add SYS_ADMIN --device /dev/fuse  -v ~/.aws:/root/.aws -v $(pwd)/app:/root/app s3fuse
 ```

# Using
```
cd /root/app
./mount_s3.1.py mount --debug
```

```
python
import iris
iris.load('/root/app/mount/mogreps-g/prods_op_mogreps-g_20160101_00_00_006.nc')
```