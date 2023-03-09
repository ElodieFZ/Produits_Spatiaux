import tools
import cdsapi
import logging
import pathlib
import yaml
import requests as r
import datetime as dt
import time

# Download raw data from the Climate Data Store
# Save to disk as netcdf file(s)
# One file = hourly data for one month for one parameter

# to not show security warnings from CDS
from requests.packages import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------
# Parameters to fill
# ---------------------------------------------------------------

# Output directory
datadir = pathlib.Path("/home", "elodie", "Data")

# Product name
# Possible values: ERA5-land / MOD11A1.061
#product = "MOD11A1.061"
product = 'ERA5-land'

# Parameters
# Possible values: total_precipitation, skin_temperature
parameters = ["total_precipitation", "skin_temperature"]
# MODIS parameters
#parameters = ['Emis_32', 'LST_Day_1km']

# Zone to download
# Existing zones: France
zone = 'France'

# Period to download - format YYYYMMDD
yyyymmdd1 = "20200101"
yyyymmdd2 = "20200215"

# Data will be saved in datadir/zone
# Filenames convention: YYYYMM_parameter.nc
# In the example: /home/elodie/Data/ERA5-land/France/202201_total_precipitation.nc

# ---------------------------------------------------------------

outdir = datadir / zone / 'TEST'

d1 = dt.datetime.strptime(yyyymmdd1, "%Y%m%d")
d2 = dt.datetime.strptime(yyyymmdd2, "%Y%m%d")

# Read zone definition
with open('zones.yaml', 'r') as f:
    bbox = yaml.safe_load(f)[zone]

starttime = time.time()

# Different download scripts depending on product
if product == "ERA5-land":

    tools.get_period_cds(product, outdir / product, parameters, d1, d2,
                             bbox['lat_min'], bbox['lat_max'], bbox['lon_min'], bbox['lon_max'])

elif product == "MOD11A1.061":

    tools.get_all_appears(outdir, product, parameters, d1, d2,
                        bbox['lat_min'], bbox['lat_max'], bbox['lon_min'], bbox['lon_max'])

else:
    print(f'Data download not available yet for product {product}')

print((time.time() - starttime))