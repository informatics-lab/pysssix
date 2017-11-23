
 - build: 
 ```
 docker build  -t s3fuse .
 ```

 - run:
 ```
 docker run -i -t  -v ~/.aws:/root/.aws -v $(pwd)/app:/root/app s3fuse
 ```

# Using
```
cd /root/app
./mount_s3.1.py mount
```

```
python
import iris
iris.load('/root/app/mount/mogreps-g/prods_op_mogreps-g_20160101_00_00_006.nc')
```

# Random stuff:


s3:///mogreps-g/prods_op_mogreps-g_20160101_00_00_006.nc

smart_open.smart_open(key)


flush /root/app/test1/prods_op_mogreps-g_20160101_00_00_006.nc (5,)
DEBUG:fuse.log-mixin:<- flush None
DEBUG:fuse.log-mixin:-> release /root/app/test1/prods_op_mogreps-g_20160101_00_00_006.nc (5,)
DEBUG:fuse.log-mixin:-> open /root/app/test1/prods_op_mogreps-g_20160101_00_00_006.nc (32768,)
DEBUG:fuse.log-mixin:<- release None
DEBUG:fuse.log-mixin:<- open 5
DEBUG:fuse.log-mixin:-> getattr /root/app/test1/prods_op_mogreps-g_20160101_00_00_006.nc (None,)
DEBUG:fuse.log-mixin:<- getattr {'st_atime': 1511369446.0, 'st_ctime': 1511369374.0, 'st_gid': 0, 'st_mode': 33188, 'st_mtime': 1511367154.0, 'st_nlink': 1, 'st_size': 36047559, 'st_uid': 0}
DEBUG:fuse.log-mixin:-> read /root/app/test1/prods_op_mogreps-g_20160101_00_00_006.nc (16384, 0, 5)
DEBUG:fuse.log-mixin:<- read 


