#!/bin/bash
# =================================================================
#
# Copyright (c) 2020 Government of Canada
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# =================================================================

BASEDIR=/data/web/msc-pygeoapi-nightly
PYGEOAPI_GITREPO=https://github.com/geopython/pygeoapi.git
MSC_PYGEOAPI_GITREPO=https://github.com/ECCC-MSC/msc-pygeoapi.git
DAYSTOKEEP=7
MSC_PYGEOAPI_OGC_API_URL=https://geomet-dev-03-nightly.cmc.ec.gc.ca/msc-pygeoapi/nightly/latest

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

cp msc-pygeoapi/deploy/default/msc-pygeoapi-config.yml msc-pygeoapi/deploy/nightly
sed -i 's#https://api.wxod-dev.cmc.ec.gc.ca/#https://geomet-dev-03-nightly/msc-pygeoapi/nightly/latest#g' msc-pygeoapi/deploy/nightly/msc-pygeoapi-config.yml
sed -i 's#basepath: /#basepath: /msc-pygeoapi/nightly/latest#' msc-pygeoapi/deploy/nightly/msc-pygeoapi-config.yml
sed -i 's^# cors: true^cors: true^' msc-pygeoapi/deploy/nightly/msc-pygeoapi-config.yml

cp msc-pygeoapi/deploy/default/msc-pygeoapi-openapi.yml msc-pygeoapi/deploy/nightly
sed -i 's#https://api.wxod-dev.cmc.ec.gc.ca/#https://geomet-dev-03-nightly.cmc.ec.gc.ca/msc-pygeoapi/nightly/latest#g' msc-pygeoapi/deploy/nightly/msc-pygeoapi-openapi.yml

cd ..

ln -s $NIGHTLYDIR latest
