import pathlib  # For file management
# import netCDF4 as nc  # to use netcdf data
import xarray as xr
import geopandas as gpd  # to work with geospatial data ("spatial extension" to pandas)
import rasterio  # to use raster data
import rasterio.plot  # to be able to plot raster data
import rioxarray as rio  # Rasterio xarray extension
import shapely.geometry  # To work with geometries
import pandas as pd
import logging
import datetime as dt
import concurrent.futures
import requests as r
import threading
import json
import time
import yaml

###
# Tools to work with spatial data in python
###

logger = logging.getLogger(__name__)
thread_local = threading.local()

def get_one_month_cds(parameter, year, month, lat_min, lat_max, lon_min, lon_max, dataset, outfile, connex):
    # Download hourly data for one full month for one parameter and one bounding box
    # Save as netcdf file
    if dataset == "ERA5-land":
        cds_dataset = "reanalysis-era5-land"
    else:
        logger.error("Unknown dataset.")
        return False

    logger.info(f"Requesting {parameter} for year {year} month {month}")

    connex.retrieve(
        cds_dataset,
        {
            'area': [
                lat_max, lon_min, lat_min, lon_max,
            ],
            'variable': parameter,
            'year': year,
            'month': month,
            'day': [
                '01', '02', '03',
                '04', '05', '06',
                '07', '08', '09',
                '10', '11', '12',
                '13', '14', '15',
                '16', '17', '18',
                '19', '20', '21',
                '22', '23', '24',
                '25', '26', '27',
                '28', '29', '30',
                '31',
            ],
            'time': [
                '00:00', '01:00', '02:00',
                '03:00', '04:00', '05:00',
                '06:00', '07:00', '08:00',
                '09:00', '10:00', '11:00',
                '12:00', '13:00', '14:00',
                '15:00', '16:00', '17:00',
                '18:00', '19:00', '20:00',
                '21:00', '22:00', '23:00',
            ],
            'format': 'netcdf',
        },
        outfile)
    logger.info(f"Data saved to {outfile}")

    return True


def get_period_cds(dataset, outdir, parameter, yyyymmdd1, yyyymmdd2, lat_min, lat_max, lon_min, lon_max, connex):
    # Download hourly data for one period, one parameter and one bounding box
    # Save as netcdf files, one file per month

    assert (outdir.is_dir())

    for y in pd.date_range(yyyymmdd1, yyyymmdd2, freq="MS"):
        year = y.strftime("%Y")
        month = y.strftime("%m")
        outfile = outdir / f"{year}{month}_{parameter}.nc"
        get_one_month_cds(parameter, year, month, lat_min, lat_max, lon_min, lon_max, dataset, outfile, connex)


def read_shapefile(file, crs_out='EPSG:4326'):
    """
    Reads a shapefile, return as a geopandas object in a specific projection
    :param file: pathlib path
    :param crs_out: str, projection in output, default is 'EPSG:4326'
    :return: GeoDataframe
    """

    assert (file.is_file())

    shape = gpd.read_file(file)

    if len(shape) > 1:
        logger.error(f"More than one geometry available in shapefile {file}")
        logger.error("Exiting as we do not know which one to use")
        exit(1)

    # If undefined CRS, it is assumed to be the default output one
    if shape.crs is None:
        shape.crs = crs_out
    # Reproject if necessary
    elif shape.crs != crs_out:
        logger.info(f"Reprojecting from {shape.crs} to {crs_out}")
        shape = shape.to_crs(crs_out)

    return shape


def geojson_from_bbox(lat_min, lat_max, lon_min, lon_max):
    """
    Create geojson object from bbox coordinates
    """
    lon_point_list = [lon_min, lon_min, lon_max, lon_max]
    lat_point_list = [lat_min, lat_max, lat_max, lat_min]
    geom = shapely.geometry.Polygon(zip(lon_point_list, lat_point_list))
    gdf = gpd.GeoDataFrame(index=[0], crs='epsg:4326', geometry=[geom])
    return json.loads(gdf.to_json())


def create_appears_download_task(dataset, parameter, yyyymmdd1, yyyymmdd2, zone):
    """
    Return formatted json request in AppEEARS format
    """

    return {
        'task_type': 'area',
        'task_name': '_'.join(['request', dataset, parameter,
                               yyyymmdd1.strftime('%Y%m%d'), yyyymmdd2.strftime('%Y%m%d')]),
        'params': {
            'dates': [
                {
                    # Dates format MM-DD-YYYY
                    'startDate': yyyymmdd1.strftime("%m-%d-%Y"),
                    'endDate': yyyymmdd2.strftime("%m-%d-%Y")
                }],
            'layers': [{'layer': parameter, 'product': dataset}],
            'output': {
                'format': {
                    'type': 'geotiff'},
                'projection': 'geographic'},
            'geo': zone,
        }
    }


def get_all_appears(outdir, dataset, parameters, yyyymmdd1, yyyymmdd2, lat_min, lat_max, lon_min, lon_max):
    """
    Download data from Appears API for:
        - one product
        - one or several layers of the product
        - a defined bbox
        - a time period
    """

    # Convert latitude-longitude to geojson zone
    zone = geojson_from_bbox(lat_min, lat_max, lon_min, lon_max)

    # Create list of download tasks
    tasks_list = []
    # Download 10 days maximum per task
    nb_days_per_task = 10
    # watch out dernier jour du mois ou premier du suivant ?
    for day1 in pd.date_range(yyyymmdd1, yyyymmdd2, freq=f"{nb_days_per_task}D"):
        for p in parameters:
            day2 = min(yyyymmdd2 + dt.timedelta(days=1), day1 + dt.timedelta(days=nb_days_per_task - 1))
            tasks_list.append(create_appears_download_task(dataset, p, day1, day2, zone))

    # Send all data requests to API at once
    tasks_ids = send_request_appeears(tasks_list)

    i = 0
    for task in tasks_list:
        print(f"Processing task {task['task_name']}")
        # request_appears(task, outdir / dataset)
        request_appears(task, outdir / dataset / f"task_{i}")
        i += 1


def send_request_appears(list, api='https://appeears.earthdatacloud.nasa.gov/api/'):


def get_credentials(portal, file="../../config.yml"):
    with open(file, 'r') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)[portal]
    return config['username'], config['password']

def request_appears(task, outdir, api='https://appeears.earthdatacloud.nasa.gov/api/'):
    """
    Send a json request to AppEEARS API.
    Wait for it to be completed.
    Save data to disk.
    """

    out = outdir / task['params']['layers'][0]['layer']
    out.mkdir(exist_ok=True, parents=True)
    (out / 'aux').mkdir(exist_ok=True)

    user, password = get_credentials('AppEEARS')
    token_response = r.post('{}login'.format(api), auth=(user, password)).json()
    head = {'Authorization': 'Bearer {}'.format(token_response['token'])}

    # Send request to API
    task_response = r.post('{}task'.format(api), json=task, headers=head).json()

    # Wait for request completion
    starttime = time.time()
    while r.get('{}task/{}'.format(api, task_response['task_id']), headers=head).json()['status'] != 'done':
        print(r.get('{}task/{}'.format(api, task_response['task_id']), headers=head).json()['status'])
        time.sleep(20.0 - ((time.time() - starttime) % 20.0))
    print(r.get('{}task/{}'.format(api, task_response['task_id']), headers=head).json()['status'])

    # Download data from request
    # todo: download only the _param_ tif files? and drop the QC and aux stuff?
    bundle = r.get(f'{api}bundle/{task_response["task_id"]}', headers=head).json()
    for f in bundle['files']:

        dl = r.get(f"{api}bundle/{task_response['task_id']}/{f['file_id']}", headers=head, stream=True,
                   allow_redirects='True')
        if f['file_name'].endswith('.tif'):
            fileout_path = out / f['file_name'].split('/')[1]
        else:
            fileout_path = out / 'aux' / f['file_name']

        logger.info(f'Création de {fileout_path}')
        with open(fileout_path, 'wb') as ff:
            for data in dl.iter_content(chunk_size=8192):
                ff.write(data)

                # Convert time from UTC to local time

# Décumul de précipitations

# Conversion unités
