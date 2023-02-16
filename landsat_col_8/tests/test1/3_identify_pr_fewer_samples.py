import pandas as pd
import os

'''
    Config Session
'''

PATH_FILE = 'data/samples.csv'

CLASSES = {
    'Forest Formation':'#006400', 
    'Pasture':'#ffd966', 
    'Agriculture':'#e974ed', 
    'Grassland':'#b8af4f',
    'Savanna Formation':'#00ff00',
    'Wetland':'#45c2a5' 
}

PATH_FILE = os.path.abspath('./landsat_col_8/tests/test1/data/samples.csv')
OUTPUT_FILE = os.path.abspath('./landsat_col_8/tests/test1/data/samples_count_by_pr.csv')

'''
    Input Data
'''

def run():

    file = pd.read_csv(PATH_FILE)

    # create item number
    file['ITEM'] = 1
    file['PR'] = file.apply(lambda serie: str(serie['PR']), 1)

    # n samples by PR and LEGEND
    file_samples_class = file.groupby(by=['PR', 'YEAR','LEGEND']).count()
    file_samples_class = file_samples_class.reset_index().rename(columns={'Unnamed: 0': 'N_SAMPLES'})
    file_samples_class = file_samples_class[['PR', 'YEAR','LEGEND', 'N_SAMPLES']]

    # count number of samples by PR
    file_samples_pr = file_samples_class.groupby(by=['PR', 'YEAR']).sum()\
        .reset_index()\
        .drop_duplicates(subset=['N_SAMPLES'])\
        .sort_values(by=['YEAR','N_SAMPLES'])[['PR','N_SAMPLES']]

    # export samples
    file_samples_pr.to_csv(OUTPUT_FILE)



if __name__ == '__main__':
    run()