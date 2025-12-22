MAILTO=""
# =================================================================
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
# =================================================================

# every day at 0300h, clean bulletin records from ES older than 30 days
0 3 * * * geoadm . /local/home/geoadm/.profile && msc-pygeoapi data bulletins-realtime clean-indexes --days 30 --yes

# every day at 0400h, clean hydrometric realtime data older than 30 days
0 4 * * * geoadm . /local/home/geoadm/.profile && msc-pygeoapi data hydrometric-realtime clean-indexes --days 30 --yes

# every hour on the 04, cache hydrometric stations
4 * * * * geoadm . /local/home/geoadm/.profile && msc-pygeoapi data hydrometric-realtime cache-stations

# every day at 0500h, clean swob realtime data older than 30 days
0 5 * * * geoadm . /local/home/geoadm/.profile && msc-pygeoapi data swob-realtime clean-indexes --days 30 --yes

# every day at 0600h, clean aqhi realtime data older than 3 days
0 6 * * * geoadm . /local/home/geoadm/.profile && msc-pygeoapi data aqhi-realtime clean-indexes --dataset all --days 3 --yes

# every day at 0700h, clean metnotes data older than 7 days
0 7 * * * geoadm . /local/home/geoadm/.profile && msc-pygeoapi data metnotes clean-indexes --days 7 --yes

# every day at 0800h clean hurricanes data older than 30 days
0 8 * * * geoadm . /local/home/geoadm/.profile && msc-pygeoapi data hurricanes clean-indexes --days 30 --dataset all --yes

# every hour update hurricanes active status (48 hours from publication datetime)
0 * * * * geoadm . /local/home/geoadm/.profile && msc-pygeoapi data hurricanes update-active-status -h 48

# every day at 0300h, clean out empty MetPX directories
0 3 * * * geoadm . /local/home/geoadm/.profile && /usr/bin/find $MSC_PYGEOAPI_CACHEDIR -type d -empty -delete > /dev/null 2>&1

# every hour, clean out CitypageWeather XML files older than 12 hours
0 * * * * geoadm . /local/home/geoadm/.profile && /usr/bin/find $MSC_PYGEOAPI_CACHEDIR/citypage_weather -type f -mmin +720 -delete > /dev/null 2>&1

# every hour, clean out Marine Weather XML files older than 12 hours
0 * * * * geoadm . /local/home/geoadm/.profile && /usr/bin/find $MSC_PYGEOAPI_CACHEDIR/marine_weather -type f -mmin +720 -delete > /dev/null 2>&1

# every day at 0800h, clean umos realtime data older than 7 days
0 8 * * * geoadm . /local/home/geoadm/.profile && msc-pygeoapi data umos-realtime clean-indexes --dataset all --days 7 --yes

# every hour at 00h, clean expired thunderstorm outlooks
0 * * * * geoadm . /local/home/geoadm/.profile && msc-pygeoapi data thunderstorm-outlook clean-outlooks --yes

# every hour at 00h, clean expired tcoastal flood risk index
0 * * * * geoadm . /local/home/geoadm/.profile && msc-pygeoapi data coastal-flood-risk-index clean-index --yes

# every hour at 00h, clean alerts indexes older than 2 hours
0 * * * * geoadm . /local/home/geoadm/.profile && msc-pygeoapi data alerts-realtime clean-indexes --yes

