import sys
sys.path.append("../commonfiles/python")
import os
import requests
from datetime import datetime, timedelta
import time
import logging.config
from multiprocessing import Queue, Event, Process

from multi_process_logging import MainLogConfig
from MultiProcDataSaver import MPDataSaver
from xeniaSQLAlchemy import xeniaAlchemy, multi_obs, platform
from xenia_obs_map import obs_map, json_obs_map
from unitsConversion import uomconversionFunctions

import optparse
if sys.version_info[0] < 3:
  import ConfigParser
else:
  import configparser as ConfigParser




source_to_xenia = [
  {
    'header_column':'wind_speed',
    'source_uom': 'm_s-1',
    'target_obs': 'wind_speed',
    'target_uom': 'm_s-1',
    's_order': 1
  },
  {
  'header_column': 'wind_from_direction',
    'source_uom': 'degrees_true',
    'target_obs': 'wind_from_direction',
    'target_uom': 'degrees_true',
    's_order': 1
  },
  {
    'header_column': 'air_temperature',
    'source_uom': 'celsius',
    'target_obs': 'air_temperature',
    's_order': 1,
    'target_uom': 'celsius'
  },
  {
    'header_column': 'sea_water_practical_salinity',
    'source_uom': 'psu',
    'target_obs': 'salinity',
    's_order': 1,
    'target_uom': 'psu'
  },
  {
    'header_column': 'air_pressure',
    'source_uom': 'mb',
    'target_obs': 'air_pressure',
    's_order': 1,
    'target_uom': 'mb'
  },
  {
    'header_column': 'relative_humidity',
    'source_uom': 'percent',
    'target_obs': 'relative_humidity',
    's_order': 1,
    'target_uom': 'percent'
  },
  {
    'header_column': 'sea_water_temperature',
    'source_uom': 'celsius',
    'target_obs': 'water_temperature',
    's_order': 1,
    'target_uom': 'celsius'
  }

]

def main():
  parser = optparse.OptionParser()
  parser.add_option("--ConfigFile", dest="ini_file",
                    help="Configuration file" )
  parser.add_option("--StartDate", dest="start_date",
                    help="Optional starting date/time to use for query" )
  parser.add_option("--EndDate", dest="end_date",
                    help="Optional ending date/time to use for query" )
  (options, args) = parser.parse_args()

  config_file = ConfigParser.RawConfigParser()
  config_file.read(options.ini_file)
  #logging_queue = Queue()
  log_file = config_file.get('logging', 'config_file')
  logger_name = 'CORMPProcessing'
  log_conf =MainLogConfig(log_file, logger_name)
  log_conf.setup_logging()
  '''
  log_stop_event = Event()
  log_listener = Process(target=queue_listener_process,
                             name='listener',
                             args=(logging_queue, log_stop_event, log_conf.config_dict(), logger_name))

  log_listener.start()
  client_log = ClientLogConfig(logging_queue)
  logging.config.dictConfig(client_log.config_dict())
  '''

  logger = logging.getLogger(logger_name)
  logger.info("Log file opened.")
  try:
    base_url = config_file.get('cormp_settings', 'url')
    platforms = config_file.get("platforms", "names").split(',')
    db_settings_ini = config_file.get('password_settings', 'config_file')
    units_conversion_file = config_file.get('units', 'config_file')
    uom_converter = uomconversionFunctions(units_conversion_file)

    db_config_file = ConfigParser.RawConfigParser()
    db_config_file.read(db_settings_ini)
    db_user = db_config_file.get('Database', 'user')
    db_pwd = db_config_file.get('Database', 'password')
    db_host = db_config_file.get('Database', 'host')
    db_name = db_config_file.get('Database', 'name')
    db_connectionstring = db_config_file.get('Database', 'connectionstring')
    db_obj = xeniaAlchemy()
    if (db_obj.connectDB(db_connectionstring, db_user, db_pwd, db_host, db_name, False) == True):
      logger.info("Succesfully connect to DB: %s at %s" % (db_name, db_host))
    else:
      logger.error("Unable to connect to DB: %s at %s. Terminating script." % (db_name, db_host))

  except ConfigParser.Error as e:
    logger.exception(e)
  else:
    data_queue = Queue()
    data_saver = MPDataSaver(data_queue, log_conf.logging_queue, db_settings_ini)
    #data_saver = MPDataSaver(data_queue, logging_queue, db_settings_ini)
    data_saver.start()

    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=1)

    start_processing_time = time.time()
    row_entry_date = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    for platform_handle in platforms:
      platform_parts = platform_handle.split('.')
      obs_mapping = json_obs_map()
      obs_mapping.load_json(source_to_xenia)
      obs_mapping.build_db_mappings(platform_handle=platform_handle,
                                    db_user=db_user,
                                    db_password=db_pwd,
                                    db_host=db_host,
                                    db_name=db_name,
                                    db_connectionstring=db_connectionstring)
      # Get platform info for lat/long
      plat_rec = db_obj.session.query(platform) \
        .filter(platform.platform_handle == platform_handle) \
        .one()

      url = base_url.format(platform_name=platform_parts[1].lower(),start_time=start_time.strftime('%Y-%m-%d %H:%M:%S'), end_time=end_time.strftime('%Y-%m-%d %H:%M:%S'))

      try:
        logger.debug("Querying platform: %s Start: %s End: %s" % (platform_parts[1].lower(),
                                                                 start_time.strftime('%Y-%m-%d %H:%M:%S'),
                                                                 end_time.strftime('%Y-%m-%d %H:%M:%S')))
        req = requests.get(url)
        if req.status_code == 200:
          time_series = req.json()
          parameters = time_series['properties']['parameters']
          quality_decode = time_series['properties']['quality_levels']

          for parameter in parameters:
            obs_nfo = obs_mapping.get_rec_from_source_name(str(parameter['id']))
            if obs_nfo is not None:
              for ndx,value in enumerate(parameter['observations']['values']):
                quality_code = parameter['observations']['quality_levels'][ndx]
                m_date = parameter['observations']['times'][ndx]
                if obs_nfo.target_uom != obs_nfo.source_uom:
                  value = uom_converter.measurementConvert(value, obs_nfo.source_uom, obs_nfo.target_uom)
                db_rec = multi_obs(row_entry_date=row_entry_date,
                                   platform_handle=platform_handle,
                                   sensor_id=(obs_nfo.sensor_id),
                                   m_type_id=(obs_nfo.m_type_id),
                                   m_date=m_date,
                                   m_lon=plat_rec.fixed_longitude,
                                   m_lat=plat_rec.fixed_latitude,
                                   m_value=value
                                   )
                data_saver.add_records([db_rec])
            else:
              logger.error("Source obs: %s not found in mapping." % (str(parameter['id'])))
        else:
          logger.error("GET code: %d error in request" % (req.status_code))
      except Exception as e:
        logger.exception(e)

  logger.info("Finished processing in %f seconds" % (time.time()-start_processing_time))

  logger.info("Signalling worker process to shutdown")
  data_saver.add_records([None])
  data_saver.join()

  logger.info("Closing log file.")
  log_conf.shutdown_logging()
  #log_stop_event.set()
  #log_listener.join()

  return
if __name__ == "__main__":
  main()