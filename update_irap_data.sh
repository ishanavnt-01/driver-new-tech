#!/bin/sh

cd /var/www/driver_new_tech
chmod a+x runcron.sh
docker exec "driver-new-tech" ./manage.py update_irap_data >> irap_test.log


