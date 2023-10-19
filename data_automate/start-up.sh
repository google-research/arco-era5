#! /bin/bash
command="docker exec -it \\\$(docker ps -qf name=arco-era5-raw-to-zarr-to-bq) /bin/bash"
sudo sh -c "echo \"$command\" >> /etc/profile"