#!/bin/bash
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

# pygeoapi entry script

echo "START /entrypoint.sh"

set +e

# gunicorn env settings with defaults
SCRIPT_NAME="/"
CONTAINER_NAME="msc-pygeoapi-nightly"
CONTAINER_HOST=${CONTAINER_HOST:=0.0.0.0}
CONTAINER_PORT=${CONTAINER_PORT:=5089}
WSGI_WORKERS=${WSGI_WORKERS:=4}
WSGI_WORKER_TIMEOUT=${WSGI_WORKER_TIMEOUT:=6000}
WSGI_WORKER_CLASS=${WSGI_WORKER_CLASS:=gevent}

# What to invoke: default is to run gunicorn server
entry_cmd=${1:-run}

# Shorthand (bash)
function error() {
    echo "ERROR: $@"
}

echo "Trying to generate OpenAPI document with PYGEOAPI_CONFIG=${PYGEOAPI_CONFIG} and PYGEOAPI_OPENAPI=${PYGEOAPI_OPENAPI}..."
pygeoapi openapi generate ${PYGEOAPI_CONFIG} --output-file ${PYGEOAPI_OPENAPI}
# pygeoapi openapi validate ${PYGEOAPI_OPENAPI}

if [ $? -ne 0 ]; then
    error "OpenAPI document could not be generated ERROR"
    DEFAULT_PYGEOAPI_OPENAPI=`echo ${PYGEOAPI_OPENAPI} | sed 's|deploy/nightly|deploy/default|'`
    echo "Using default OpenAPI document with DEFAULT_PYGEOAPI_OPENAPI=${DEFAULT_PYGEOAPI_OPENAPI}"
    cp ${DEFAULT_PYGEOAPI_OPENAPI} ${PYGEOAPI_OPENAPI}
fi

echo "OpenAPI document generated. continue to pygeoapi..."

case ${entry_cmd} in
    # Run pygeoapi server
    run)
        # SCRIPT_NAME should not have value '/'
        [[ "${SCRIPT_NAME}" = '/' ]] && export SCRIPT_NAME="" && echo "make SCRIPT_NAME empty from /"

        echo "Start gunicorn name=${CONTAINER_NAME} on ${CONTAINER_HOST}:${CONTAINER_PORT} with ${WSGI_WORKERS} workers and SCRIPT_NAME=${SCRIPT_NAME}"
        exec gunicorn --workers ${WSGI_WORKERS} \
                --worker-class=${WSGI_WORKER_CLASS} \
                --timeout ${WSGI_WORKER_TIMEOUT} \
                --name=${CONTAINER_NAME} \
                --bind ${CONTAINER_HOST}:${CONTAINER_PORT} \
                --reload \
                --reload-extra-file ${PYGEOAPI_CONFIG} \
                pygeoapi.flask_app:APP
      ;;
    *)
      error "unknown command arg: must be 'run'"
      ;;
esac

echo "END /entrypoint.sh"
