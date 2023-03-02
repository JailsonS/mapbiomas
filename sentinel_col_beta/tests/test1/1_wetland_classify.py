import ee
import os
import numpy as np
import pandas as pd
import random

from pprint import pprint

from modules.index import getFractions, getNdfi, getCsfi
from modules.util import removeCloudShadow, shuffle

ee.Initialize()

pd.options.mode.chained_assignment = None  # default='warn'

'''
    Config Section
'''

ASSET_SAMPLES = 'projects/imazon-simex/LULC/SAMPLES/COLLECTION-S2/C7/TRAINED'
ASSET_TILES = 'projects/mapbiomas-workspace/AUXILIAR/SENTINEL2/grid_sentinel'
ASSET_BIOMES = 'projects/mapbiomas-workspace/AUXILIAR/biomas-2019'
ASSET_OUTPUT = 'projects/imazon-simex/LULC/SENTINEL-2-CBETA/wetland'

OUTPUT_VERSION = '1'

FILEPATH_REF_AREA = os.path.abspath(
    './sentinel_col_beta/tests/test1/data/areas_c71_grid_sentinel.csv')

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
    [11, 0],
    [15, 0],
    [12, 0],
    [25, 0],
    [33, 1],
    [18, 0]
])

# proportion data
N_SAMPLES = 3000

PROPORTION_SAMPLES = pd.DataFrame([
    {'class':  0, 'min_samples': N_SAMPLES * 0.75, 'proportion': 0.75},
    {'class':  1, 'min_samples': N_SAMPLES * 0.25, 'proportion': 0.25},
])

TILES = []

YEARS = [
    # 2016,
    # 2017,
    # 2018,
    2019,
    # 2020, # <-- não rodar 2020
    # 2021,
    # 2022,
]

BANDS = ['B2', 'B3', 'B4', 'B8', 'B11', 'B12', 'QA60']
NEW_BAND_NAMES = ['blue', 'green', 'red', 'nir', 'swir1', 'swir2', 'pixel_qa']

FEAT_SPACE_BANDS = [
    "gv", "gvs", "soil", "npv", "ndfi", "csfi",
    "ndfi_max", "ndfi_min",
    "soil_max", "soil_min",
    "gv_max", "gv_min",
    "ndfi_median", "soil_median", "gv_median"
]


# model parameters
RF_PARAMS = {
    'numberOfTrees': 60,
    # 'variablesPerSplit': 4,
    # 'minLeafPopulation': 25
}

'''
    Auxiliar Function
'''


def getProportionTable(table: pd.DataFrame, tile: str, year: int) -> pd.DataFrame:

    tablePerc = table.query("year == {} & tile == '{}'".format(year, tile))

    #tablePerc['area_ha'] = (tablePerc['area_ha']).astype(float).round(decimals=4)
    tablePerc['area_percent'] = (
        tablePerc['area_ha'] / tablePerc['area_ha'].sum()).round(decimals=4)
    tablePerc['n_samples'] = tablePerc['area_percent'].mul(
        N_SAMPLES).round().astype(int)

    # join ref area with proportion
    df = pd.merge(tablePerc, PROPORTION_SAMPLES, how="outer", on="class")

    # compare to min samples: rule (min_samples > n_samples = min_samples)
    df.loc[df['min_samples'] > df['n_samples'],
           'n_samples'] = df['min_samples']
    df = df.replace(float("NaN"), 0)

    return df


def getReferenceAreaTable(tile, year):

    referenceTable = pd.read_csv(FILEPATH_REF_AREA)[
        ['tile', 'gridname', 'year', 'class', 'area_ha']]

    # normalize classes
    referenceTable = referenceTable.replace({'class': {
        9: 0, 30: 0, 50: 0, 19: 0, 32: 0, 20: 0, 41: 0, 11: 0,
        3: 0, 4: 0, 15: 0, 12: 0, 25: 0, 33: 1, 21: 0,
        24: 0, 39: 0, 62: 0
    }}).groupby(by=['tile', 'gridname', 'year', 'class']).sum().reset_index()

    referenceTable = getProportionTable(referenceTable, str(tile), int(year))

    # pprint(referenceTable.head())

    return referenceTable


def removeShadow(image):
    threshShade = image.select('shade').gt(0.8)
    threshNdfi = image.select('ndfi').gt(0.5)
    # treshGv = image.select('ndvi').gt(0.5).and(image.select('ndfi').lt(0.4));

    mask = threshShade.And(threshNdfi)

    return image.mask(mask.eq(0))


def trainRandomTileImg(randomTiles: list, year: int) -> ee.featurecollection.FeatureCollection:

    pprint(randomTiles)

    def trainOneImageByTile(year, tile) -> ee.featurecollection.FeatureCollection:

        dfReferenceArea = getReferenceAreaTable(tile, year)

        grid = dfReferenceArea['gridname'].values[0]

        assetRandomTile = '{}/{}-{}-{}'.format(
            ASSET_SAMPLES, grid, str(year), '5')

        sp = ee.FeatureCollection(assetRandomTile).remap(
            CLASS_REMAP[:, 0:1].flatten().tolist(),
            CLASS_REMAP[:, 1:2].flatten().tolist(),
            'class'
        ).select(['class'])

        colTile = (ee.ImageCollection('COPERNICUS/S2_HARMONIZED')
                   .filterDate(str(year) + '-01-01', str(year) + '-12-30')
                   .filter('CLOUDY_PIXEL_PERCENTAGE <= 50')
                   .filter(ee.Filter.eq('MGRS_TILE', tile))
                   .map(lambda image:
                        image.select('B2', 'B3', 'B4', 'B8', 'B11', 'B12')
                        .divide(10000).addBands(image.select('QA60'))
                        .copyProperties(image))
                   .select(BANDS, NEW_BAND_NAMES)
                   .sort('CLOUDY_PIXEL_PERCENTAGE')
                   .map(getFractions)
                   .map(getNdfi)
                   .map(getCsfi)
                   .map(removeCloudShadow)
                   .map(removeShadow)
                )

        minMax = colTile.select(['ndfi', 'gv', 'soil']).reduce(ee.Reducer.minMax())
        median = colTile.select(['ndfi', 'gv', 'soil']).reduce(ee.Reducer.median())

        imageTile = ee.Image(colTile.first()).addBands(minMax).addBands(median)\
            .select(FEAT_SPACE_BANDS)

        samplesRandomTileTrain = imageTile.sampleRegions(
            collection=sp,
            scale=10
        )

        return samplesRandomTileTrain

    listTrainRandom = map(
        lambda tile: trainOneImageByTile(year, tile), randomTiles)

    trained = ee.FeatureCollection(list(listTrainRandom)).flatten()

    return trained


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

for year in YEARS:

    for tile in TILES:

        alreadyInCollection = ee.ImageCollection(ASSET_OUTPUT)\
            .filter('tile == "{}"'.format(tile))\
            .reduceColumns(ee.Reducer.toList(), ['id_image']).get('list')\
            .getInfo()


        currentTile = ee.Feature(tilesCollection.filter(
            ee.Filter.eq('NAME', tile)).first())

        geometry = currentTile.geometry()

        collection = (ee.ImageCollection('COPERNICUS/S2_HARMONIZED')
                      .filterDate(str(year) + '-01-01', str(year) + '-12-30')
                      .filter('CLOUDY_PIXEL_PERCENTAGE <= 50')
                      .filter(ee.Filter.eq('MGRS_TILE', tile))
                      .map(lambda image:
                           image.select('B2', 'B3', 'B4', 'B8', 'B11', 'B12')
                           .divide(10000).addBands(image.select('QA60'))
                           .copyProperties(image))
                      .select(BANDS, NEW_BAND_NAMES)
                      .map(getFractions)
                      .map(getNdfi)
                      .map(getCsfi)
                      .map(removeCloudShadow)
                      .map(removeShadow)
                      )

        minMax = collection.select(['ndfi', 'gv', 'soil']).reduce(ee.Reducer.minMax())
        median = collection.select(['ndfi', 'gv', 'soil']).reduce(ee.Reducer.median())

        listIdImages = collection.reduceColumns(ee.Reducer.toList(), ['system:index']).get('list')\
            .getInfo()

        listIdImages = list(set(listIdImages) - set(alreadyInCollection))

        for idImage in listIdImages:
            try:

                tile = tile

                image = ee.Image(collection.filter(ee.Filter.eq('system:index', idImage)).first())\
                    .addBands(minMax)\
                    .addBands(median)
                image = image.select(FEAT_SPACE_BANDS)

                yearSample = year - 1 if year == 2022 else year

                # proportion table area
                dfReferenceArea = getReferenceAreaTable(tile, yearSample)

                gridName = dfReferenceArea['gridname'].values[0]

                # samples from one tile
                assetTileSamples = '{}/{}-{}-{}'.format(ASSET_SAMPLES, gridName, str(yearSample), '5')
                samplesTile = ee.FeatureCollection(assetTileSamples).remap(
                    CLASS_REMAP[:, 0:1].flatten().tolist(),
                    CLASS_REMAP[:, 1:2].flatten().tolist(),
                    'class'
                ).select(['class'])

                # train samples current tile
                samplesTileTrain = image.sampleRegions(
                    collection=samplesTile,
                    scale=10
                )

                # train samples random images
                tilesRandom = list(random.choices(TILES, k=15))
                samplesTilesRandom = trainRandomTileImg(tilesRandom, yearSample)

                # filter samples based on proportion
                samplesToModel = ee.FeatureCollection(samplesTilesRandom.merge(samplesTileTrain))
                samplesToModel = shuffle(collection=samplesToModel)

                dfReferenceArea['samples_gee'] = dfReferenceArea.apply(
                    lambda serie: samplesToModel.filter(ee.Filter.eq(
                        'class', serie['class'])).limit(serie['n_samples']),
                    axis=1
                )

                # get trainning samples
                samplesToModel = ee.FeatureCollection(list(dfReferenceArea['samples_gee'].values)).flatten()

                # create model
                model = ee.Classifier.smileRandomForest(**RF_PARAMS)\
                    .train(samplesToModel, 'class', FEAT_SPACE_BANDS)

                # predict image
                classification = image.classify(model)

                # set properties
                classification = classification\
                    .set('version', OUTPUT_VERSION)\
                    .set('tile', tile)\
                    .set('grid_name', str(gridName))\
                    .set('year', year)\
                    .set('id_image', idImage)\
                    .byte()

                name = '{}_{}_{}_{}_{}'.format(idImage, str(
                    tile), gridName, str(year), OUTPUT_VERSION)
                assetId = '{}/{}'.format(ASSET_OUTPUT, name)

                pprint('Exporting... ' + name)

                task = ee.batch.Export.image.toAsset(
                    image=classification,
                    description=name,
                    assetId=assetId,
                    scale=10,
                    region=geometry,
                    maxPixels=1e13
                )

                task.start()

            except Exception as e:
                print(e)
