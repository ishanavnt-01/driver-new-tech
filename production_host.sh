sudo apt-get update
sudo apt-get install docker-compose

sudo apt-get update
sudo apt-get install nginx

sudo apt install postgresql-client -y

sudo mkdir -p /var/log/django
sudo chmod -R 777 /var/log/django

sudo chmod 777 /var/run/docker.sock

cd /var/www/
git clone git@github.com:hsarbas/DRIVER2.0.git
sudo chmod 777 driver-new-tech/

cd ~
sudo rm -rf driver-new-tech/

cd /var/www/
sudo mkdir media
sudo chmod 777 media/
cd /var/www/media/
sudo mkdir incident_errorlog_data
sudo mkdir multi-language
sudo chmod 777 incident_errorlog_data/ multi-language/

(crontab -l ; echo "0 6 * * * /var/www/driver-new-tech/runcron.sh >> /var/www/driver-new-tech/finding_duplicate_records.log")| crontab -

(crontab -l ; echo "0 0 */7 * * /var/www/driver-new-tech/update_irap_data.sh >> /var/www/driver-new-tech/update_irap_data.log")| crontab -

(crontab -l ; echo "0 */6 * * * /var/www/driver-new-tech/bulk_upload.sh >> /var/www/driver-new-tech/bulk_log.log")| crontab -
