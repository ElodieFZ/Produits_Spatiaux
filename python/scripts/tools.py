import pathlib  # For file management
#import netCDF4 as nc  # to use netcdf data
import xarray as xr
import geopandas as gpd  # to work with geospatial data ("spatial extension" to pandas)
import rasterio  # to use raster data
import rasterio.plot  # to be able to plot raster data
import rioxarray as rio  # Rasterio xarray extension
import shapely.geometry  # To work with geometries
import pandas as pd
import logging
import datetime as dt

###
# Tools to work with spatial data in python
###

logger = logging.getLogger(__name__)


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


def get_period_cds(dataset, outdir, parameter, yyyymm1, yyyymm2, lat_min, lat_max, lon_min, lon_max, connex):
    # Download hourly data for one period, one parameter and one bounding box
    # Save as netcdf files, one file per month

    assert(outdir.is_dir())

    for y in pd.date_range(dt.datetime.strptime(yyyymm1, "%Y%m"), dt.datetime.strptime(yyyymm2, "%Y%m"), freq="MS"):
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

    assert(file.is_file())

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


# Convert time from UTC to local time

# Décumul de précipitations

# Conversion unités




