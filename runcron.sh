#! /bin/bash

# virtualenv is now active, which means your PATH has been modified.
# Don't try to run python from /usr/bin/python, just run "python" and
# let the PATH figure out which version to run (based on what your
# virtualenv has configured).
cd /var/www/driver_new_tech
chmod a+x runcron.sh
docker exec "driver-new-tech" ./manage.py find_duplicate_records >> test.log
#export DJANGO_SETTINGS_MODULE=DRIVER.settings
#export PYTHONPATH=$PWD
#python scripts/bulk_upload/add_incidents_v3_dev.py
#docker exec "driver-new-tech" ./manage.py add_incidents
#docker exec "driver-new-tech" ./manage.py load_black_spots
