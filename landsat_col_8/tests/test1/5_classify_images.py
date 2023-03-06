import ee
import os.path
import os, io
import requests, json, random
import pandas as pd

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from modules.index import getFractions, getNdfi, getCsfi
from modules.util import applyScaleFactorsL8L9, removeCloudShadow, removeShadow

from pprint import pprint

ee.Initialize()



'''
    Config Session
'''

# output info
OUTPUT_ASSET = ''
VERSION = '1'

# google drive api
SCOPES = ['https://www.googleapis.com/auth/drive']
URL_TOKEN = 'https://oauth2.googleapis.com/token'



# assets
ASSET_PR = 'projects/mapbiomas-workspace/AUXILIAR/landsat-scenes'
ASSET_BIOMES = 'projects/mapbiomas-workspace/AUXILIAR/biomas-2019'
ASSET_TILES = 'projects/mapbiomas-workspace/AUXILIAR/landsat-mask'
ASSET_OUTPUT = ''


# table of reference
REF_AREA = os.path.abspath('./landsat_col_8/tests/test1/data/areas_c71.csv')

# table support table
SUPPORT_SAMPLES = './landsat_col_8/tests/test1/data/support_samples/support_samples_{}_{}_1.csv'

# normalize names
BANDS = ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7', 'QA_PIXEL', 'ST_B10']
NEW_BAND_NAMES = ['blue','green','red','nir','swir1','swir2','pixel_qa','tir']


LEGEND_NAMES = {
    'CANA': 18,
    'LAVOURA TEMPORÁRIA': 18, 
    'APICUM': 32, 
    'NÃO OBSERVADO': 27,
    'MANGUE': 3, # mangue[5] -> floresta[3]
    'RIO, LAGO E OCEANO': 33,
    'OUTRA ÁREA NÃO VEGETADA': 25,
    'PASTAGEM': 15,
    'FORMAÇÃO FLORESTAL': 3,
    'LAVOURA PERENE': 18,
    'SILVICULTURA': 3, # sivicultura[9] -> floresta[3]
    'INFRAESTRUTURA URBANA': 25, # infra urbana[24] -> outra área não vegetada[25]
    'FORMAÇÃO CAMPESTRE': 12,
    'MINERAÇÃO': 25, # mineração[30] -> outra área não vegetada[25]
    'FLORESTA INUNDÁVEL': 3, # floresta inundável[x] -> floresta[3]
    'AQUICULTURA': 33, # aquicultura[x] -> água[33]
    'FORMAÇÃO SAVÂNICA': 4,
    'VEGETAÇÃO URBANA': 25,
    'CAMPO ALAGADO E ÁREA PANTANOSA': 11,
    'AFLORAMENTO ROCHOSO': 25 # afloramento rochoso[29] -> outra área não vegetada[25]
}

FIELDS = [
    'CLASS_1985', 'CLASS_1986','CLASS_1987', 'CLASS_1988', 'CLASS_1989', 'CLASS_1990',
    'CLASS_1991', 'CLASS_1992','CLASS_1993', 'CLASS_1994', 'CLASS_1995', 'CLASS_1996',
    'CLASS_1997', 'CLASS_1998','CLASS_1999', 'CLASS_2000', 'CLASS_2001', 'CLASS_2002',
    'CLASS_2003', 'CLASS_2004','CLASS_2005', 'CLASS_2006','CLASS_2007', 'CLASS_2008',
    'CLASS_2009', 'CLASS_2010','CLASS_2011', 'CLASS_2012','CLASS_2013', 'CLASS_2014',
    'CLASS_2015', 'CLASS_2016','CLASS_2017', 'CLASS_2018','CLASS_2019', 'CLASS_2020',
    'CLASS_2021'
]




# model parameters
RF_PARAMS = {
    'numberOfTrees': 50,
    # 'variablesPerSplit': 4,
    # 'minLeafPopulation': 25
}

FEAT_SPACE_BANDS = ["gv", "gvs", "soil", "npv", "ndfi", "csfi"]
    


# sample balance
N_SAMPLES = 2000

PROPORTION_SAMPLES = pd.DataFrame([
    {'class':  3, 'min_samples': N_SAMPLES * 0.40, 'proportion': 0.40},
    {'class':  4, 'min_samples': N_SAMPLES * 0.05, 'proportion': 0.05},
    {'class': 12, 'min_samples': N_SAMPLES * 0.05, 'proportion': 0.05},
    {'class': 15, 'min_samples': N_SAMPLES * 0.23, 'proportion': 0.25},
    {'class': 18, 'min_samples': N_SAMPLES * 0.10, 'proportion': 0.10},
    #{'class': 11, 'min_samples': N_SAMPLES * 0.05, 'proportion': 0.05},
    {'class': 33, 'min_samples': N_SAMPLES * 0.10, 'proportion': 0.10},
    {'class': 25, 'min_samples': N_SAMPLES * 0.02, 'proportion': 0.05},
])





# run params
YEARS = [
    '2022'
]

TEST_PR = [
  # "225060",
  # "224060",
  # "228061",
  "223062",
  # "227062",
  # "224066",
  # "225066",
  # "224068",
  # "231069",
  # "230069"
]


def auth():
    creds = None

    pathToken = os.path.abspath('./landsat_col_8/tests/test1/config/token.json')
    pathCred = os.path.abspath('./landsat_col_8/tests/test1/config/credentials.json')

    if os.path.exists(pathToken):
        creds = Credentials.from_authorized_user_file(pathToken, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(pathCred, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(pathToken, 'w') as token:
            token.write(creds.to_json())

    return creds

def getFile(creds):
    accessToken = creds.token

    service = build('drive', 'v3', credentials=creds)

    folderId = '1c8BqAiENhc-kXWQOKRw_9wNjtNI6IE0y'

    query = f"parents = '{folderId}'"

    response = service.files().list(q=query).execute()

    files = response.get('files')
    nextPageToken = response.get('nextPageToken')

    while nextPageToken:
        response = service.files().list(q=query, pageToken=nextPageToken).execute()
        files.extend(response.get('files'))
        nextPageToken = response.get('nextPageToken')

    dfFiles = pd.DataFrame(files)

    fileId = dfFiles.iloc[0]['id']


    url = "https://www.googleapis.com/drive/v3/files/" + fileId + "?alt=media"

    res = requests.get(url, headers={"Authorization": "Bearer " + accessToken})
    res.encoding = 'utf-8'

    return pd.read_csv(io.StringIO(res.text))


def getProportionTable(table: pd.DataFrame, tile: int, year: int) -> pd.DataFrame:
    
    tablePerc = table.query("year == {} & tile == {}".format(year, tile))
    
    tablePerc['area_ha'] = (tablePerc['area_ha']).astype(float).round(decimals=4)
    tablePerc['area_percent'] = (tablePerc['area_ha'] / tablePerc['area_ha'].sum()).round(decimals=4)
    tablePerc['n_samples'] = tablePerc['area_percent'].mul(N_SAMPLES).round().astype(int)


    # join ref area with proportion
    df = pd.merge(tablePerc, PROPORTION_SAMPLES, how="outer", on="class")
    
    # compare to min samples: rule (min_samples > n_samples = min_samples)
    df.loc[df['min_samples'] > df['n_samples'], 'n_samples'] = df['min_samples']
    df = df.replace(float("NaN"), 0)

    return df

def getNormalizedSamples(file):

    listOfSamples = []

    for index, row in file.iterrows():

        geojson = json.loads(row['.geo'])

        feat = ee.Feature(ee.Geometry.Point(geojson['coordinates']))
        feat = feat.set('PR', row['PR']).set('LANDSAT_SCENE_ID', row['LANDSAT_SCENE_ID'])

        for year in FIELDS:
            y = str(year.replace('CLASS_',''))
            feat = feat.set('label_' + y, LEGEND_NAMES[row[year]])

        for band in FEAT_SPACE_BANDS:
            feat = feat.set(band, row[band])

        listOfSamples.append(feat)

    return ee.FeatureCollection(listOfSamples)

def getReferenceAreaTable(tile, year):
   
    referenceTable = pd.read_csv(REF_AREA)[['tile', 'year','class', 'area_ha']]

    # normalize classes
    referenceTable = referenceTable.replace({'class': {
        9:3,
        30:25,
        50:3,
        19:18,
        32:18,
        20:18,
        41:18,
        11:12
    }}).groupby(by=['tile', 'year','class']).sum().reset_index()

    referenceTable = getProportionTable(referenceTable, int(tile), int(year))

    return referenceTable

def getRandomTileSample(randomTiles: list) -> ee.featurecollection.FeatureCollection:

    def _getSamples(tile):

        p = SUPPORT_SAMPLES.format((year), tile)
        dfSupport = pd.read_csv(os.path.abspath(p))\

        idImg = dfSupport['landsat_id_scene'].drop_duplicates().values
        idImg = random.choice(idImg)

        fileTileImgSupport = dfSupport.query('landsat_id_scene == "{}"'.format(idImg))

        return tableToFeatureCollection(fileTileImgSupport)

    mapRandomList = map(lambda tile: _getSamples(tile), randomTiles)
    return ee.FeatureCollection(list(mapRandomList)).flatten()

def tableToFeatureCollection(table: pd.DataFrame) -> ee.featurecollection.FeatureCollection:
    table = table[FEAT_SPACE_BANDS + ['classification']]
    listOfSuppSp = []

    for index, row in table.iterrows():
        listOfSuppSp.append(ee.Feature(None, row.to_dict()))

    return ee.FeatureCollection(listOfSuppSp)\
        .select(FEAT_SPACE_BANDS + ['classification'], FEAT_SPACE_BANDS + ['class'])


def shuffle(collection, seed=1):
    """
    Adds a column of deterministic pseudorandom numbers to a collection.
    The range 0 (inclusive) to 1000000000 (exclusive).
    """

    collection = collection.randomColumn('random', seed)\
        .sort('random', True)\
        .map(lambda feature: feature.set(
            'new_id',
            ee.Number(feature.get('random')).multiply(1000000000).round()
        )
    )

    #listIdRandom =  collection.reduceColumns(ee.Reducer.toList(), ['new_id']).get('list')

    # list of random ids
    #randomIdList = ee.List(listIdRandom)

    # list of sequential ids
    #sequentialIdList = ee.List.sequence(1, collection.size())

    # set new ids
    #shuffled = collection.remap(randomIdList, sequentialIdList, 'new_id')

    return collection
'''
    Input Data
'''
# tiles collection
tilesCollection = ee.ImageCollection(ASSET_TILES)

# get google drive credentials
cred = auth()

# get sample file
file = getFile(cred)


# iterate over years
for year in YEARS:

    y = int(year) - 1 if year == '2022' else int(year)

    label = 'label_' + str(y)

    fileYear = file.query('YEAR == {}'.format(year))

    prList = TEST_PR # list(fileYear['PR'].drop_duplicates().values)

    for tile in prList:

        pprint(0)

        fileSamplesTile = fileYear.query('PR == {}'.format(tile))

        tilesCollectionTile = tilesCollection.filter(ee.Filter.eq('tile', int(tile)))
        tileMask = ee.Image(tilesCollectionTile.first())

        geometry = tileMask.geometry()

        listIdImages = fileSamplesTile['LANDSAT_SCENE_ID'].drop_duplicates().values

        for idImg in listIdImages:

            sensor = idImg[:3]

            image = None

            if sensor == 'LC8':
                image = ee.Image(
                    ee.ImageCollection('LANDSAT/LC08/C02/T1_L2')\
                        .filter(ee.Filter.eq('LANDSAT_SCENE_ID', idImg)).first()
                )
                
            if sensor == 'LC9':
                image = ee.Image(
                    ee.ImageCollection('LANDSAT/LC09/C02/T1_L2')\
                        .filter(ee.Filter.eq('LANDSAT_SCENE_ID', idImg)).first()
                )

            image = applyScaleFactorsL8L9(image)
            image = image.select(BANDS, NEW_BAND_NAMES)
            image = ee.Image(getFractions(image)).select(['gv', 'npv', 'soil', 'cloud', 'shade', 'pixel_qa'])
            image = getNdfi(image)
            image = getCsfi(image)
            image = removeCloudShadow(image)
            image = image.select(FEAT_SPACE_BANDS)


            # get reference table
            refTableArea = getReferenceAreaTable(tile, y)
            
            fileImageSamples = fileSamplesTile.query('LANDSAT_SCENE_ID == "{}"'.format(idImg))

            # orig samples in current tile (for training)
            samplesTileImage = getNormalizedSamples(fileImageSamples)\
                .select(FEAT_SPACE_BANDS + [label], FEAT_SPACE_BANDS + ['class'])
 
            # train origin samples
            samplesTileImageTrain = image.sampleRegions(
                collection=samplesTileImage,  
                scale=30
            )

            # get support samples in current tile
            pathSupportSp = SUPPORT_SAMPLES.format((year), tile)
            fileImageSamplesSupport = pd.read_csv(os.path.abspath(pathSupportSp))\
                .query('landsat_id_scene == "{}"'.format(idImg))
            samplesSupportTileImage = tableToFeatureCollection(fileImageSamplesSupport)
            

            # get random support samples
            randomIdImages = random.choices(prList, k=15)
            samplesSupportRandom = getRandomTileSample(randomIdImages)

            allSamples = ee.FeatureCollection(samplesSupportRandom.merge(samplesSupportTileImage).merge(samplesTileImageTrain))
            allSamples = shuffle(allSamples)

            refTableArea['samples_gee'] = refTableArea.apply(
                lambda serie: allSamples.filter(ee.Filter.eq(
                    'class', serie['class'])).limit(serie['n_samples']),
                axis=1
            )

            # get trainning samples
            samplesToModel = ee.FeatureCollection(list(refTableArea['samples_gee'].values)).flatten()

            # create model
            model = ee.Classifier.smileRandomForest(**RF_PARAMS)\
                .train(samplesToModel, 'class', FEAT_SPACE_BANDS)

            # predict image
            classification = image.classify(model)


            # set properties
            classification = classification\
                .set('version', VERSION)\
                .set('tile', tile)\
                .set('year', year)\
                .set('id_image', idImage)\
                .byte()

            name = '{}_{}_{}_{}_{}'.format(idImg, str(tile), str(year), VERSION)
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