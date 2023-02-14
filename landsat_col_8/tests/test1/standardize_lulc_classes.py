import pandas as pd

import os.path
import sys, os, pprint, re, io
import requests


from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

#sys.path.insert(0, os.path.abspath('./landsat_col_8/tests'))

'''
    Config session
'''

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive']


LEGEND_NAMES = {
    'CANA': 'Agriculture',
    'LAVOURA TEMPORÁRIA': 'Agriculture',
    'APICUM': 'Apicum',
    'NÃO OBSERVADO': 'Non Observed',
    'MANGUE': 'Mangrove',
    'RIO, LAGO E OCEANO': 'Water Bodies',
    'OUTRA ÁREA NÃO VEGETADA': 'Other Non Vegetated Area',
    'PASTAGEM': 'Pasture',
    'FORMAÇÃO FLORESTAL': 'Forest Formation',
    'LAVOURA PERENE': 'Agriculture',
    'SILVICULTURA': 'Agriculture',
    'INFRAESTRUTURA URBANA': 'Urban',
    'FORMAÇÃO CAMPESTRE': 'Grassland',
    'MINERAÇÃO': 'Mining',
    'FLORESTA INUNDÁVEL': 'Flooded Forest',
    'AQUICULTURA': 'Aquicultura',
    'FORMAÇÃO SAVÂNICA': 'Savanna Formation',
    'VEGETAÇÃO URBANA': 'Urban Vegetation',
    'CAMPO ALAGADO E ÁREA PANTANOSA': 'Wetland',
    'AFLORAMENTO ROCHOSO': 'Rock Outcrops'
}

YEARS = list(range(1985, 2022, 1))

LAPIG_CLASS_NAMES = list(map(lambda item: 'CLASS_' + str(item), YEARS))

URL_TOKEN = 'https://oauth2.googleapis.com/token'

OUTPUT_FILE = os.path.abspath('./landsat_col_8/tests/test1/data/samples.csv')

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

    fileId = dfFiles.iloc[-1]['id']

    url = "https://www.googleapis.com/drive/v3/files/" + fileId + "?alt=media"

    res = requests.get(url, headers={"Authorization": "Bearer " + accessToken})
    res.encoding = 'utf-8'

    return pd.read_csv(io.StringIO(res.text))

'''
   Init Program
'''

def main():

    try:

        # auth google drive api and get credentials
        creds = auth()

        # get most recent uploaded file
        dfSamples = getFile(creds)

        # melt table
        dfSamplesMelted = pd.melt(
            frame=dfSamples, 
            id_vars=['PR', 'LANDSAT_SCENE_ID'],
            value_vars=LAPIG_CLASS_NAMES, 
            value_name='LEGEND_LAPIG', 
            var_name='YEAR'
        )
        
        dfSamplesMelted['YEAR'] = dfSamplesMelted.apply(lambda serie: int(serie['YEAR'].split('_')[1]), 1)

        # standardize legend
        dfSamplesMelted = dfSamplesMelted.replace({'LEGEND_LAPIG': LEGEND_NAMES})

        dfSamplesMelted = dfSamplesMelted.rename(columns={'LEGEND_LAPIG': 'LEGEND'})

        # export csv
        dfSamplesMelted.to_csv(OUTPUT_FILE)
        print('success')

    except HttpError as error:
        # TODO(developer) - Handle errors from drive API.
        print(f'An error occurred: {error}')



if __name__ == '__main__':
    main()