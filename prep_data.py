# ========== (c) JP Hwang 16/7/21  ==========

import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

desired_width = 320
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', desired_width)

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
sh.setFormatter(formatter)
root_logger.addHandler(sh)

import dask.dataframe as dd
import pandas as pd
import pyproj
from pyproj import Transformer
import requests
import os

cell_towers_path = "temp/cell_towers.csv"

ddf = dd.read_csv(cell_towers_path)
ddf.head()



# Categorize radio
ddf['radio'] = ddf.radio.astype('category')

# Created and updated to datetime integers
ddf['created'] = dd.to_datetime(ddf.created, unit='s').astype('int')
ddf['updated'] = dd.to_datetime(ddf.updated, unit='s').astype('int')

# Filter out outliers created before 2003
ddf = ddf[dd.to_datetime(ddf.created) >= '2003']

# convert lon/lat to epsg:3857 (psuedo-mercator) so generated images
# can be overlayed on a Mercator projected map
transformer = Transformer.from_crs("epsg:4326", "epsg:3857")
def to3857(df):
    x_3857, y_3857 = transformer.transform(df.lat.values, df.lon.values)
    return df.assign(x_3857=x_3857, y_3857=y_3857)

ddf = ddf.map_partitions(to3857)

ddf.head()

# Download network info for mcc/mnc from 'https://cellidfinder.com/mcc-mnc'
html = requests.get('https://cellidfinder.com/mcc-mnc')
tables = pd.read_html(html.content)
mcc_mnc_df = pd.concat(tables).reset_index(drop=True)

# Create description column as Network, falling back to "Operator or branch" if Network not found
mcc_mnc_df['Description'] = mcc_mnc_df.Network.where(
    ~pd.isnull(mcc_mnc_df.Network), mcc_mnc_df['Operator or brand name']
)

# Drop unneeded columns
codes = mcc_mnc_df.drop(['Network', 'Operator or brand name'], axis=1)
codes.head()



# Categorize non-numeric columns
for col, dtype in codes.dtypes.items():
    if dtype == 'object':
        codes[col] = codes[col].astype('category')

# Merge mnc/mcc info with cell towers dataset
ddf_merged = ddf.merge(codes, left_on=['mcc', 'net'], right_on=['MCC', 'MNC'], how='left')
ddf_merged

# Write parquet file to ../data directory
parquet_path = 'temp/cell_towers.parq'
ddf_merged.to_parquet(parquet_path, compression=None)

ddf_sm = ddf_merged.sample(frac=0.1)
sm_parquet_path = 'temp/cell_towers_sm.parq'
ddf_sm.to_parquet(sm_parquet_path, compression=None)
