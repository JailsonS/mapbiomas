import ee
import numpy as np
import pandas as pd

'''
    Config Section
'''

ASSET_SAMPLES = 'projects/imazon-simex/LULC/SAMPLES/COLLECTION-S2/C7/TRAINED'
ASSET_MOSAICS = 'projects/nexgenmap/MapBiomas2/SENTINEL/mosaics-3'
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

'''
    Input Data
'''

collection = ee.ImageCollection(ASSET_MOSAICS) \
    .filterMetadata('version', 'equals', MOSAIC_VERSION) \
    .filterMetadata('biome', 'equals', 'AMAZONIA')

if len(TILES) == 0:
    TILES = collection.aggregate_histogram('grid_name').getInfo().keys()

alreadyInCollection = ee.ImageCollection(ASSET_OUTPUT)\
    .filter('version == "{}"'.format(OUTPUT_VERSION))\
    .reduceColumns(ee.Reducer.toList(), ['system:index']).get('list').getInfo()

'''
    Iterate Years
'''

for year in YEARS:

    collectionYear = collection.filter('year == {}'.format(year))

    for tile in TILES:
        imageName = '{}-{}-{}'.format(tile, year, OUTPUT_VERSION)

        if imageName not in alreadyInCollection:
            try:
                samplesName = ''





            except Exception as e:
                print(e)