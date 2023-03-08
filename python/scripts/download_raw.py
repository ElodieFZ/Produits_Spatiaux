import tools
import cdsapi
import logging
import pathlib
import yaml

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
# Possible values: ERA5-land
product = "ERA5-land"

# Parameters
# Possible values: total_precipitation, skin_temperature
parameters = ["total_precipitation", "skin_temperature"]

# Zone to download
# Existing zones: France
zone = 'France'

# Period to download (full months at hourly resolution)
date1 = "202001"
date2 = "202112"

# Data will be saved in datadir/zone
# Filenames convention: YYYYMM_parameter.nc
# In the example: /home/elodie/Data/ERA5-land/France/202201_total_precipitation.nc

# ---------------------------------------------------------------

outdir = datadir / product / zone

# Read zone definition
with open('zones.yaml', 'r') as f:
    bbox = yaml.safe_load(f)[zone]

# Different download scripts depending on product
if product == "ERA5-land":

    # Connect to CDS
    c = cdsapi.Client()

    for p in parameters:
        tools.get_period_cds(product, outdir, p, date1, date2,
                             bbox['lat_min'], bbox['lat_max'], bbox['lon_min'], bbox['lon_max'], c)
