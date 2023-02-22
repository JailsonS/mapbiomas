import ee

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import os.path
import os, io, pprint
import requests, json, random
import pandas as pd

from modules.index import getFractions, getNdfi, getCsfi
from modules.util import applyScaleFactorsL8L9, removeCloudShadow

ee.Initialize()

'''
    Config Session
'''

# output info
OUTPUT = 'users/jailson/mapbiomas/samples'
VERSION = '1'



# assets
ASSET_TRAIN_SAMPLES = 'projects/imazon-simex/LULC/COLLECTION8/SAMPLES/lapig_samples_w_edge_and_edited_amazonia_v1_train'
ASSET_PR = 'projects/mapbiomas-workspace/AUXILIAR/landsat-scenes'
ASSET_BIOMES = 'projects/mapbiomas-workspace/AUXILIAR/biomas-2019'
ASSET_TILES = 'projects/mapbiomas-workspace/AUXILIAR/landsat-mask'




# google drive api
SCOPES = ['https://www.googleapis.com/auth/drive']
URL_TOKEN = 'https://oauth2.googleapis.com/token'



# table of reference
REF_AREA = os.path.abspath('./landsat_col_8/tests/test1/data/areas_c71.csv')

# random points
PTS_FILE = os.path.abspath('./landsat_col_8/tests/test1/data/random_points.csv')



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
  # "223062",
  # "227062",
  "224066",
  "225066",
  "224068",
  "231069",
  "230069"
]


'''
    Aux functions
'''
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




'''
    Main function
'''
if __name__ == '__main__':
    try:

        # get google drive credentials
        cred = auth()

        # get sample file
        file = getFile(cred)

        # tiles collection
        tilesCollection = ee.ImageCollection(ASSET_TILES)


        # iterate over years
        for year in YEARS:

            y = int(year) - 1 if year == '2022' else int(year)
            label = 'label_' + str(y)
    

            fileYear = file.query('YEAR == {}'.format(year))

            prList = TEST_PR # list(fileYear['PR'].drop_duplicates().values)

            for tile in prList:

                fileSamplesTile = fileYear.query('PR == {}'.format(tile))

                tilesCollectionTile = tilesCollection.filter(ee.Filter.eq('tile', int(tile)))
                tileMask = ee.Image(tilesCollectionTile.first())

                geometry = tileMask.geometry()

                listIdImages = fileSamplesTile['LANDSAT_SCENE_ID'].drop_duplicates().values
                
                # random samples
                randomPoints = ee.FeatureCollection.randomPoints(
                    region=geometry, 
                    points=1000
                )

                pprint.pprint(len(listIdImages))
                
                for idImg in listIdImages:
                    
                    fileImageSamples = fileSamplesTile.query('LANDSAT_SCENE_ID == "{}"'.format(idImg))

                    samplesRandomImages = random.choices(listIdImages, k=5)
                    fileRandomImage = fileSamplesTile.loc[fileSamplesTile['LANDSAT_SCENE_ID'].isin(samplesRandomImages)]


                    samplesTileImage = getNormalizedSamples(fileImageSamples)
                    samplesRandomImages = getNormalizedSamples(fileRandomImage)

                    allSamples = samplesRandomImages.merge(samplesTileImage)


                    # get reference table
                    refTableArea = getReferenceAreaTable(tile, y)

                    # filter samples according balance samples
                    refTableArea['samples_gee'] = refTableArea.apply(
                        lambda serie: allSamples.filter(ee.Filter.eq(label, serie['class'])).limit(serie['n_samples']),
                        axis=1
                    )

                    # get trainning samples
                    trainingSamples = ee.FeatureCollection(list(refTableArea['samples_gee'].values)).flatten()

                    trainingSamples = trainingSamples.select(FEAT_SPACE_BANDS + [label])

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


                    # train random samples
                    randomPointsTrain = image.sampleRegions(
                        collection=randomPoints,  
                        scale=30, 
                        geometries=True
                    )

                    # create model
                    model = ee.Classifier.smileRandomForest(**RF_PARAMS)\
                        .train(trainingSamples, label, FEAT_SPACE_BANDS)

                    supportSamples = randomPointsTrain.classify(model)

                    urlSamples = supportSamples.getDownloadURL(
                        selectors=FEAT_SPACE_BANDS + ['classification']
                    )

                    r = requests.get(urlSamples, stream=True)

                    if r.status_code != 200:
                        r.raise_for_status()

                    dfSupportSamples = pd.read_csv(io.StringIO(r.content.decode('utf-8')))

                    dfSupportSamples['tile'] = tile
                    dfSupportSamples['landsat_id_scene'] = idImg


                
                description = 'support_samples_{}_{}_{}'.format(str(year), str(tile), VERSION)











        



    except HttpError as error:
        # TODO(developer) - Handle errors from drive API.
        print(f'An error occurred: {error}')





