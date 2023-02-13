import pandas as pd

import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


'''
    Config session
'''

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']

LEGEND_NAMES = {
    'CANA': 'Agriculture',
    'LAVOURA PERENE': 'Agriculture',
    'SILVICULTURA': 'Agriculture',
    'LAVOURA TEMPORÁRIA': 'Agriculture',
    'APICUM': 'Apicum',
    'NÃO OBSERVADO': 'Non Observed',
    'MANGUE': 'Mangrove',
    'RIO, LAGO E OCEANO': 'Water Bodies',
    'OUTRA ÁREA NÃO VEGETADA': 'Other Non Vegetated Area',
    'PASTAGEM': 'Pasture',
    'FORMAÇÃO FLORESTAL': 'Forest Formation',
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

YEARS = list(range(1985, 2023, 1))

LAPIG_CLASS_NAMES = list(map(lambda item: 'CLASS_' + str(item), YEARS))


'''
   Init Program
'''





def main():

    creds = None

    if os.path.exists('config/token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        print('invalid credentials!')
    else:
        print('logged')

        try:
            service = build('drive', 'v3', credentials=creds)

            folderId = ''

            query = f"parents = '{folderId}'"

            response = service.files().list(q=query).execute()
            files = response.get('files')
            nextPageToken = response.get('nextPageToken')

            while nextPageToken:
                response = service.files().list(q=query, pageToken=nextPageToken).execute()
                files.extend(response.get('files'))
                nextPageToken = response.get('nextPageToken')

            dfFiles = pd.DataFrame(files)
            print(dfFiles)

            

        except HttpError as error:
            # TODO(developer) - Handle errors from drive API.
            print(f'An error occurred: {error}')



if __name__ == '__main__':
    main()