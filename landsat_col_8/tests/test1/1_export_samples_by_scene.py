import ee

from utils.index import getNdfi, getFractions

ee.Initialize()


'''
    Config Data
'''

YEAR = '2022'

ASSET_TRAIN_SAMPLES = 'projects/imazon-simex/LULC/COLLECTION8/SAMPLES/lapig_samples_w_edge_and_edited_amazonia_v1_train'

ASSET_PR = 'projects/mapbiomas-workspace/AUXILIAR/landsat-scenes'
ASSET_BIOMES = 'projects/mapbiomas-workspace/AUXILIAR/biomas-2019'

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

BANDS = ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7', 'QA_PIXEL', 'ST_B10']
NEW_BAND_NAMES = ['blue','green','red','nir','swir1','swir2','pixel_qa','tir']

'''
    Auxiliar Functions
'''

def applyScaleFactorsL8L9(image: ee.image.Image) -> ee.image.Image:
    opticalBands = image.select('SR_B.').multiply(0.0000275).add(-0.2)
    thermalBands = image.select('ST_B.*').multiply(0.00341802).add(149.0)
    return image.addBands(opticalBands, None, True)\
                .addBands(thermalBands, None, True)

def removeCloudShadow(image: ee.image.Image) -> ee.image.Image:
  
    cloudThreshould = image.select('cloud').lt(0.23)
    
    qa = image.select('pixel_qa')
    
    cloudBitMask = 1 << 4
    shadeBitMask = 1 << 3
    
    mask = qa.bitwiseAnd(cloudBitMask).eq(0).And(qa.bitwiseAnd(shadeBitMask).eq(0))
    
    return image.mask(cloudThreshould).mask(mask)


'''
    Get all samples by scene
'''

amzBiome = ee.FeatureCollection(ASSET_BIOMES)\
    .filter('Bioma == "Amazônia"')

samplesDataset = ee.FeatureCollection(ASSET_TRAIN_SAMPLES)

def extractSamplesByPr(pr):

    region = ee.FeatureCollection(ASSET_PR).filter('PR == "' + pr + '"')
    
    center = ee.Feature(region.first()).centroid()

    col8 = (ee.ImageCollection('LANDSAT/LC08/C02/T1_L2')
        .filterBounds(center.geometry())
        .filterDate(YEAR + '-01-01', YEAR + '-12-30')
        .filter('CLOUD_COVER <= 50')
        .map(lambda image: image.set('sensor', 'L8'))
        .map(applyScaleFactorsL8L9)
        .select(BANDS, NEW_BAND_NAMES)
        .map(getFractions)
        .map(getNdfi)
    )

    col9 = (ee.ImageCollection('LANDSAT/LC09/C02/T1_L2')
        .filterBounds(center.geometry())
        .filterDate(YEAR + '-01-01', YEAR + '-12-30')
        .filter('CLOUD_COVER <= 50')
        .map(lambda image: image.set('sensor', 'L8'))
        .map(applyScaleFactorsL8L9)
        .select(BANDS, NEW_BAND_NAMES)
        .map(getFractions)
        .map(getNdfi)
    )

    collection = col8.merge(col9).map(removeCloudShadow)

    listIdScene = collection.reduceColumns(ee.Reducer.toList(), ['LANDSAT_SCENE_ID']).get('list').getInfo()
    listSceneData = []

    for idScene in listIdScene:

        currentScene = ee.Image(collection.filter(ee.Filter.eq('LANDSAT_SCENE_ID', idScene)).first())

        sampleValues = currentScene.sampleRegions(
            collection=samplesDataset,  
            scale=30, 
            geometries=False
        )

        sampleValues = sampleValues\
            .map(lambda feat: feat.set('LANDSAT_SCENE_ID', idScene))\
            .map(lambda feat: feat.set('PR', pr))


        listSceneData.append(sampleValues)

    datasetSampleValues = ee.FeatureCollection(listSceneData).flatten()

    return datasetSampleValues

mapPr = map(lambda pr: extractSamplesByPr(pr), TEST_PR)

dataset = ee.FeatureCollection(list(mapPr)).flatten()


'''
    Export data
'''
descriptionTest = 'lapig_samples_w_edge_amazonia_v1_test1'

task = ee.batch.Export.table.toDrive(
    description=descriptionTest,
    collection=dataset,
    fileFormat='CSV'
)

task.start()




