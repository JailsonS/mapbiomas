import ee
import numpy as np
import pandas as pd

from pprint import pprint

from modules.index import getFractions, getNdfi, getCsfi
from modules.util import removeCloudShadow

ee.Initialize()

'''
    Config Section
'''

ASSET_SAMPLES = 'projects/imazon-simex/LULC/SAMPLES/COLLECTION-S2/C7/TRAINED'
ASSET_TILES = 'projects/mapbiomas-workspace/AUXILIAR/SENTINEL2/grid_sentinel'
ASSET_BIOMES = 'projects/mapbiomas-workspace/AUXILIAR/biomas-2019'
ASSET_OUTPUT = ''

MOSAIC_VERSION = '3'
OUTPUT_VERSION = '1'

CLASS_VALUES = {
    3: 'forest formation',
    4: 'savanna formation',
    11: 'wetland',
    15: 'pasture',
    12: 'grassland',
    25: 'non vegetated area',
    33: 'water bodies'
}

CLASS_REMAP = np.array([
    [3, 0],
    [4, 0],
    [11, 1],
    [15, 0],
    [12, 0],
    [25, 0],
    [33, 1]
])

# proportion data
N_SAMPLES = 2000

PROPORTION_SAMPLES = pd.DataFrame([
    {'class':  0, 'min_samples': N_SAMPLES * 0.7, 'proportion': 0.7},
    {'class':  1, 'min_samples': N_SAMPLES * 0.3, 'proportion': 0.3},
])

TILES = []
    
YEARS = [
    2016,
    2017,
    2018,
    2019,
    2020,
    2021,
    2022,
]

BANDS = ['B2', 'B3', 'B4', 'B8', 'B11', 'B12', 'QA60']
NEW_BAND_NAMES = ['blue','green','red','nir','swir1','swir2', 'pixel_qa']


'''
    Input Data
'''

amazonia = ee.FeatureCollection(ASSET_BIOMES).filter('Bioma == "Amazônia"')

tilesCollection = ee.FeatureCollection(ASSET_TILES)\
    .filterBounds(amazonia)

if len(TILES) == 0:
    TILES = tilesCollection.reduceColumns(ee.Reducer.toList(), ['NAME']).get('list')\
        .getInfo()

'''
    Iterate Years
'''

for year in YEARS[:1]:

    for tile in TILES[:1]:
        
        currentTile = ee.Feature(tilesCollection.filter(ee.Filter.eq('NAME', tile)).first())

        collection = (ee.ImageCollection('COPERNICUS/S2_HARMONIZED')
            .filterDate(str(year) + '-01-01', str(year) + '-12-30')
            .filter('CLOUDY_PIXEL_PERCENTAGE <= 50')
            .filter(ee.Filter.eq('MGRS_TILE', tile))
            .map(lambda image: image.divide(10000).copyProperties(image))
            .select(BANDS, NEW_BAND_NAMES)
            .map(getFractions)
            .map(getNdfi)
            .map(getCsfi)
            .map(removeCloudShadow)
        )

        listIdImages = collection.reduceColumns(ee.Reducer.toList(), ['system:index']).get('list')\
            .getInfo()
        
        for idImage in listIdImages:
            try:

                image = ee.Image(collection.filter(ee.Filter.eq('system:index', idImage)).first())

            except Exception as e:
                print(e)

