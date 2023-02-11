import ee

ee.Initialize()


'''
    Config session
'''
ASSET_LAPIG_SAMPLES = 'projects/mapbiomas-workspace/VALIDACAO/mapbiomas_85k_col2_points_w_edge_and_edited_amazonia_v1'
ASSET_SPLITED_SAMPLES = 'projects/imazon-simex/LULC/COLLECTION8/SAMPLES'

# proportion dataset
SPLIT = 0.7



'''
    Input data
'''
datasetSamples = ee.FeatureCollection(ASSET_LAPIG_SAMPLES)\
    .filter('BIOMA == "Amazônia"')
# n_samples: 32972(100%) | 23080.3(70%)


'''
    Split data
'''
datasetSamplesShuffled = datasetSamples.randomColumn('random')
trainDataset = datasetSamplesShuffled.filter('random >= {}'.format(SPLIT))
testDataset = datasetSamplesShuffled.filter('random < {}'.format(SPLIT))



'''
    Export samples
'''
descriptionTrain = 'lapig_samples_w_edge_and_edited_amazonia_v1_train'
descriptionTest = 'lapig_samples_w_edge_and_edited_amazonia_v1_test'

taskTrain = ee.batch.Export.table.toAsset(
    description=descriptionTrain,
    assetId= '{}/{}'.format(ASSET_SPLITED_SAMPLES, descriptionTrain),
    collection=trainDataset
)

taskTest = ee.batch.Export.table.toAsset(
    description=descriptionTest,
    assetId= '{}/{}'.format(ASSET_SPLITED_SAMPLES, descriptionTest),
    collection=testDataset
)

taskTrain.start()
taskTest.start()
