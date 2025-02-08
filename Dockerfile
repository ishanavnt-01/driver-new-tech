FROM python:3.13-bookworm

RUN set -ex && \
    apt-get update && \
    apt-get install -y --no-install-recommends libgdal-dev

RUN apt-get update && apt-get install -y \
    gettext \
    libgeos-dev \
    libspatialindex-dev \
    gdal-bin \
    postgis

RUN ["gdal-config", "--version"]

RUN mkdir -p /opt/app
WORKDIR /opt/app

COPY . /opt/app

RUN pip install --no-cache-dir gunicorn
RUN pip install -r requirements.txt


CMD ["gunicorn", "-e", "DJANGO_SETTINGS_MODULE=DRIVER.settings", "DRIVER.wsgi", "-w3", "-b:4000", "-kgevent"]
