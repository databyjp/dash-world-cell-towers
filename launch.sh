#!/bin/bash
# Add this directory to PYTHONPATH
export PYTHONPATH=$PYTHONPATH:`dirname "$(realpath $0)"`

# For Dash-gallery
pip install --upgrade pip # to the latest version
pip install -r requirements-predeploy.txt  # Now, we can install them

# # -- WHEN USING COILED - UNHIDE THE NEXT TWO LINES --
python coiled_login.py  # Log into coiled using token saved as env variable
python coiled_create_env.py  # Create coiled env up front
gunicorn "dash_opencellid.app:server" --timeout 240 --workers 2
# -- END - COILED SECTION --

# -- WHEN USING LOCAL COMPUTE - UNHIDE THE NEXT THREE LINES --
# dask-scheduler &
# dask-worker 127.0.0.1:8786 --nprocs 1 --local-directory work-dir &
# python publish_data.py
# gunicorn "dash_opencellid.app:get_server()" --timeout 60 --workers 1
# -- END - FOR LOCAL COMPUTE --

wait
