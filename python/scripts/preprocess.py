import tools
import cdsapi
import logging
import pathlib

# Preprocess data 
# - raw data already available on disk
# - extract data over zone
# - decumulate rainfall
# - convert units
# - interpolate


# ---------------------------------------------------------------
# Parameters to fill
# ---------------------------------------------------------------

# Output directory
datadir = pathlib.Path("/home", "elodie", "Data")

# Dataset
# Possible values: ERA5-land
dataset = "ERA5-land"

# Parameters
# Possible values: total_precipitation, skin_temperature
parameters = ["total_precipitation", "skin_temperature"]

# Zone to download
zone = 'France'
lat_min = 42
lat_max = 52
lon_min = -6
lon_max = 9

# Period to download (full months at hourly resolution)
date1 = "202001"
date2 = "202112"

# Data will be saved in datadir/zone
# Filenames convention: YYYYMM_parameter.nc
# In the example: /home/elodie/Data/ERA5-land/France/202201_total_precipitation.nc

# ---------------------------------------------------------------

outdir = datadir / dataset / zone
assert(datadir.is_dir())
outdir.mkdir(parents=True, exist_ok=True)

france = tools.read_shapefile(datadir / "shapefiles" / "France" / "FRA_adm0.shp")
occitanie = tools.read_shapefile(datadir / "shapefiles" / "France" / "ADMIN-EXPRESS_3-1" / "REGION.shp")

