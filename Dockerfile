###################################################################
#
# Author: Tom Kralidis <tom.kralidis@ec.gc.ca>
#
# Copyright (c) 2023 Tom Kralidis
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
###################################################################

FROM ubuntu:focal

ARG PYGEOAPI_GITREPO=https://github.com/geopython/pygeoapi.git

ENV BASEDIR=/data/web/msc-pygeoapi-nightly

WORKDIR $BASEDIR

# Install system deps
RUN sed -i 's/http:\/\/archive.ubuntu.com\/ubuntu\//mirror:\/\/mirrors.ubuntu.com\/mirrors.txt/g' /etc/apt/sources.list
RUN apt-get update && \
    apt-get install -y software-properties-common  && \
    add-apt-repository ppa:gcpp-kalxas/wmo && \
    add-apt-repository ppa:ubuntugis/ppa && apt update && \
    apt-get install -y python3 python3-setuptools python3-pip git curl unzip python3-click python3-fiona python3-gdal python3-lxml python3-parse python3-pyproj python3-rasterio python3-requests python3-slugify python3-sqlalchemy python3-unicodecsv python3-xarray python3-yaml

# install pygeoapi
RUN git clone $PYGEOAPI_GITREPO -b 0.16.1 && \
    cd pygeoapi && \
    pip3 install -r requirements.txt && \
    pip3 install flask_cors gunicorn gevent greenlet && \
    python3 setup.py install && \
    cd ..

# requirement of GEOMET_CLIMATE_CONFIG file
RUN curl -O https://raw.githubusercontent.com/ECCC-CCCS/geomet-climate/master/geomet-climate.yml --create-dirs --output /opt/geomet-climate

# get latest schemas.opengis.net
RUN mkdir schemas.opengis.net && \
    curl -O http://schemas.opengis.net/SCHEMAS_OPENGIS_NET.zip && \
    unzip ./SCHEMAS_OPENGIS_NET.zip "ogcapi/*" -d schemas.opengis.net && \
    rm -f ./SCHEMAS_OPENGIS_NET.zip

# install msc-pygeoapi
COPY . $BASEDIR/msc-pygeoapi
RUN cd msc-pygeoapi && \
    # requirements.txt includes elasticsearch<8
    pip3 install -r requirements.txt && \
    pip3 install elasticsearch_dsl && \
    pip3 install -U elasticsearch_dsl && \
    pip3 install -U elasticsearch && \
    # ensure cors enabled in config
    sed -i 's^# cors: true^cors: true^' $BASEDIR/msc-pygeoapi/deploy/default/msc-pygeoapi-config.yml && \
    # GCWeb theme files
    curl -L https://github.com/wet-boew/GCWeb/releases/download/v14.6.0/themes-dist-14.6.0-gcweb.1.zip -o ./themes-gcweb.zip && \
    unzip -o ./themes-gcweb.zip "*/GCWeb/*" -d theme/static && \
    unzip -o ./themes-gcweb.zip "*/wet-boew/*" -d theme/static && \
    mv ./theme/static/themes-dist-14.6.0-gcweb ./theme/static/themes-gcweb && \
    rm -f ./themes-gcweb.zip && \
    # install msc-pygeoapi
    python3 setup.py install && \
    # show version
    MSC_PYGEOAPI_VERSION=$(dpkg-parsechangelog -SVersion) && \
    sed -i "s/MSC_PYGEOAPI_VERSION/$MSC_PYGEOAPI_VERSION/" theme/templates/_base.html && \
    # ensure i18n translation strings are compiled
    pybabel compile -d locale -l fr && \
    cd ..

# cleanup apt/build deps
RUN apt-get remove --purge -y curl unzip && \
    apt-get clean && \
    apt autoremove -y  && \
    rm -rf /var/lib/apt/lists/*

# permission fix (mark as executable)
RUN chmod +x $BASEDIR/msc-pygeoapi/docker/entrypoint.sh

# start entrypoint.sh
ENTRYPOINT [ "sh", "-c", "$BASEDIR/msc-pygeoapi/docker/entrypoint.sh" ]
