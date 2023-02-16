import ee

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import os.path
import os, io
import requests, json
import pandas as pd

from modules.index import getFractions, getNdfi, getCsfi
from modules.util import applyScaleFactorsL8L9, removeCloudShadow

ee.Initialize()

'''
    Config Session
'''

# assets
ASSET_TRAIN_SAMPLES = 'projects/imazon-simex/LULC/COLLECTION8/SAMPLES/lapig_samples_w_edge_and_edited_amazonia_v1_train'
ASSET_PR = 'projects/mapbiomas-workspace/AUXILIAR/landsat-scenes'
ASSET_BIOMES = 'projects/mapbiomas-workspace/AUXILIAR/biomas-2019'





# google drive api
SCOPES = ['https://www.googleapis.com/auth/drive']

URL_TOKEN = 'https://oauth2.googleapis.com/token'




# normalize names
BANDS = ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7', 'QA_PIXEL', 'ST_B10']
NEW_BAND_NAMES = ['blue','green','red','nir','swir1','swir2','pixel_qa','tir']

LEGEND_NAMES = {
    'CANA': 18,
    'LAVOURA TEMPORÁRIA': 18,
    'APICUM': 32,
    'NÃO OBSERVADO': 27,
    'MANGUE': 5,
    'RIO, LAGO E OCEANO': 33,
    'OUTRA ÁREA NÃO VEGETADA': 25,
    'PASTAGEM': 15,
    'FORMAÇÃO FLORESTAL': 3,
    'LAVOURA PERENE': 18,
    'SILVICULTURA': 18,
    'INFRAESTRUTURA URBANA': 24,
    'FORMAÇÃO CAMPESTRE': 12,
    'MINERAÇÃO': 30,
    'FLORESTA INUNDÁVEL': 100,
    'AQUICULTURA': 31,
    'FORMAÇÃO SAVÂNICA': 4,
    'VEGETAÇÃO URBANA': 101,
    'CAMPO ALAGADO E ÁREA PANTANOSA': 11,
    'AFLORAMENTO ROCHOSO': 29
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





RF_PARAMS = {
    'numberOfTrees': 50,
    # 'variablesPerSplit': 4,
    # 'minLeafPopulation': 25
}

FEAT_SPACE_BANDS = ["gv", "gvs", "soil", "npv", "shade", "ndfi", "csfi"]
    




YEARS = [
    '2022'
]

TEST_PR = [
  "225060",
  "224060",
  "228061",
  "223062",
  "227062",
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


'''
    Normalize function
'''
def getNormalizedSamples(file):

    listOfSamples = []

    for index, row in file.iterrows():

        geojson = json.loads(row['.geo'])

        feat = ee.Feature(ee.Geometry.Point(geojson['coordinates']))
        feat = feat.set('PR', row['PR']).set('LANDSAT_SCENE_ID', row['LANDSAT_SCENE_ID'])

        for year in FIELDS:
            y = str(year.replace('CLASS_',''))
            feat = feat.set('label_' + y, LEGEND_NAMES[row[year]])

        listOfSamples.append(feat)

    return ee.FeatureCollection(listOfSamples)


def genSupportSamples(idLandsatScene, samples, year):
    
    sensor = idLandsatScene[:3]
    
    image = None

    if sensor == 'LC8':
        image = ee.Image('LANDSAT/LC08/C02/T1_L2/{}'.format(idLandsatScene))
    if sensor == 'LC9':
        image = ee.Image('LANDSAT/LC09/C02/T1_L2/{}'.format(idLandsatScene))

    image = applyScaleFactorsL8L9(image)
    image = image.select(BANDS, NEW_BAND_NAMES)
    image = getFractions(image)
    image = getNdfi(image)
    image = getCsfi(image)

    # create model
    model = ee.Classifier.smileRandomForest(RF_PARAMS)\
        .train(samples, 'label_' + year, FEAT_SPACE_BANDS)



    return image



'''
    Main function
'''
if __name__ == '__main__':
    try:

        # get google drive credentials
        cred = auth()

        # get sample file
        file = getFile(cred)

        # iterate over years
        for yearClas in YEARS:

            fileYear = file.query('YEAR == "{}"'.format(yearClas))

            idScenes = list(fileYear['LANDSAT_SCENE_ID'].drop_duplicates().values)

            samples = getNormalizedSamples(fileYear)



        



    except HttpError as error:
        # TODO(developer) - Handle errors from drive API.
        print(f'An error occurred: {error}')





