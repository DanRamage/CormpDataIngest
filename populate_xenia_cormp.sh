#!/bin/bash

source /usr/local/virtualenv/pyenv-2.7.11/bin/activate

if [ ! -f /tmp/lock_populate_xenia_cormp ]; then
  touch /tmp/lock_populate_xenia_cormp

  startTime=`date -u`
  echo "Start time: $startTime\n" > /home/xeniaprod/tmp/log/populate_xenia_cormp.log 2>&1

  cd /home/xeniaprod/scripts/CormpDataIngest

  python /home/xeniaprod/scripts/CormpDataIngest/CORMPDataIngestion.py --ConfigFile=/home/xeniaprod/config/cormp_ingest.ini
  rm -f /tmp/lock_populate_xenia_cormp

  startTime=`date -u`
  echo "\nEnd time: $startTime" >> /home/xeniaprod/tmp/log/populate_xenia_cormp.log 2>&1

else
  startTime=`date -u`
  echo "Lock file exists, cannot run script: $startTime\n" >> /home/xeniaprod/tmp/log/populate_xenia_cormp.log 2>&1
fi