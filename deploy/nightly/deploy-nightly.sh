#!/bin/bash
# =================================================================
#
# Author: Tom Kralidis <tom.kralidis@ec.gc.ca>
#
# Copyright (c) 2022 Tom Kralidis
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
# =================================================================

BASEDIR=/data/web/msc-pygeoapi-nightly
PYGEOAPI_GITREPO=https://github.com/geopython/pygeoapi.git
MSC_PYGEOAPI_GITREPO=https://github.com/ECCC-MSC/msc-pygeoapi.git
DAYSTOKEEP=7
export MSC_PYGEOAPI_OGC_API_URL=https://geomet-dev-21-nightly.cmc.ec.gc.ca/msc-pygeoapi/nightly/latest/
export MSC_PYGEOAPI_ES_URL=http://localhost:9200
export MSC_PYGEOAPI_OGC_API_URL_BASEPATH=/
export MSC_PYGEOAPI_TEMPLATES=/data/web/msc-pygeoapi-nightly/latest/msc-pygeoapi/theme/templates
export MSC_PYGEOAPI_STATIC=/data/web/msc-pygeoapi-nightly/latest/msc-pygeoapi/theme/static
export MSC_PYGEOAPI_OGC_SCHEMAS_LOCATION=/data/web/msc-pygeoapi-nightly/latest/schemas.opengis.net
export GEOMET_HPFX_BASEPATH=/data/geomet/feeds/hpfx
export GEOMET_SCIENCE_BASEPATH=/data/geomet/feeds/cmoi-science

# you should be okay from here

DATETIME=`date +%Y%m%d`
TIMESTAMP=`date +%Y%m%d.%H%M`
NIGHTLYDIR=msc-pygeoapi-$TIMESTAMP

echo "Deleting nightly builds > $DAYSTOKEEP days old"

cd $BASEDIR

for f in `find . -type d -name "msc-pygeoapi-20*"`
do
    DATETIME2=`echo $f | awk -F- '{print $3}' | awk -F. '{print $1}'`
    let DIFF=(`date +%s -d $DATETIME`-`date +%s -d $DATETIME2`)/86400
    if [ $DIFF -gt $DAYSTOKEEP ]; then
        rm -fr $f
    fi
done

rm -fr latest
echo "Generating nightly build for $TIMESTAMP"
python3.6 -m venv --system-site-packages $NIGHTLYDIR && cd $NIGHTLYDIR
source bin/activate
git clone $MSC_PYGEOAPI_GITREPO
git clone $PYGEOAPI_GITREPO
cd pygeoapi
pip3 install -r requirements.txt
pip3 install flask_cors elasticsearch
python3 setup.py install
cd ../msc-pygeoapi
python3 setup.py install
cd ..

mkdir schemas.opengis.net
curl -O http://schemas.opengis.net/SCHEMAS_OPENGIS_NET.zip && unzip ./SCHEMAS_OPENGIS_NET.zip "ogcapi/*" -d schemas.opengis.net && rm -f ./SCHEMAS_OPENGIS_NET.zip

cp msc-pygeoapi/deploy/default/msc-pygeoapi-config.yml msc-pygeoapi/deploy/nightly
sed -i 's#basepath: /#basepath: /msc-pygeoapi/nightly/latest#' msc-pygeoapi/deploy/nightly/msc-pygeoapi-config.yml
sed -i 's^# cors: true^cors: true^' msc-pygeoapi/deploy/nightly/msc-pygeoapi-config.yml

pygeoapi openapi generate msc-pygeoapi/deploy/nightly/msc-pygeoapi-config.yml > msc-pygeoapi/deploy/nightly/msc-pygeoapi-openapi.yml

cd ..

ln -s $NIGHTLYDIR latest
