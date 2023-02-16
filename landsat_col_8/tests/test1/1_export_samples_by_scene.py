import ee

ee.Initialize()


'''
    Config Data
'''

YEARS = [
    '2022'
]

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

SELECTORS = [
    'PR', 'LANDSAT_SCENE_ID', 'YEAR','blue','green','red','nir','swir1', 
    'ndfi', 'soil', 'gv', 'gvs', 'npv', 'cloud', 
    'CLASS_1985', 'CLASS_1986','CLASS_1987', 'CLASS_1988', 'CLASS_1989', 'CLASS_1990',
    'CLASS_1991', 'CLASS_1992','CLASS_1993', 'CLASS_1994', 'CLASS_1995', 'CLASS_1996',
    'CLASS_1997', 'CLASS_1998','CLASS_1999', 'CLASS_2000', 'CLASS_2001', 'CLASS_2002',
    'CLASS_2003', 'CLASS_2004','CLASS_2005', 'CLASS_2006','CLASS_2007', 'CLASS_2008',
    'CLASS_2009', 'CLASS_2010','CLASS_2011', 'CLASS_2012','CLASS_2013', 'CLASS_2014',
    'CLASS_2015', 'CLASS_2016','CLASS_2017', 'CLASS_2018','CLASS_2019', 'CLASS_2020',
    'CLASS_2021', '.geo'
]

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

def getFractions(image: ee.image.Image) -> ee.image.Image:

    # default endmembers
    ENDMEMBERS = [
            [0.0119,0.0475,0.0169,0.625,0.2399,0.0675], # GV
            [0.1514,0.1597,0.1421,0.3053,0.7707,0.1975], # NPV
            [0.1799,0.2479,0.3158,0.5437,0.7707,0.6646], # Soil
            [0.4031,0.8714,0.79,0.8989,0.7002,0.6607] #  Cloud
    ]

    outBandNames = ['gv', 'npv', 'soil', 'cloud']
    
    
    fractions = ee.Image(image).select(['blue', 'green', 'red', 'nir', 'swir1', 'swir2'])\
        .unmix(ENDMEMBERS)\
        .max(0)


    fractions = fractions.rename(outBandNames)

    summed = fractions.expression('b("gv") + b("npv") + b("soil")')

    shade = summed.subtract(1.0).abs().rename("shade")

    fractions = fractions.addBands(shade)

    return image.addBands(fractions)

def getNdfi(image: ee.image.Image) -> ee.image.Image:

    summed = image.expression('b("gv") + b("npv") + b("soil")')

    gvs = image.select("gv").divide(summed).rename("gvs")

    npvSoil = image.expression('b("npv") + b("soil")')

    ndfi = ee.Image.cat(gvs, npvSoil)\
            .normalizedDifference()\
            .rename('ndfi')

    image = image.addBands(gvs)
    image = image.addBands(ndfi)

    return ee.Image(image)


'''
    Get all samples by scene
'''

amzBiome = ee.FeatureCollection(ASSET_BIOMES)\
    .filter('Bioma == "Amazônia"')

samplesDataset = ee.FeatureCollection(ASSET_TRAIN_SAMPLES)

def extractSamplesByPr(pr):

    region = ee.FeatureCollection(ASSET_PR).filter('PR == "' + pr + '"')
    
    center = ee.Feature(region.first()).centroid()

    listSceneData = []

    for y in YEARS:

        col8 = (ee.ImageCollection('LANDSAT/LC08/C02/T1_L2')
            .filterBounds(center.geometry())
            .filterDate(y + '-01-01', y + '-12-30')
            .filter('CLOUD_COVER <= 50')
            .map(lambda image: image.set('sensor', 'L8'))
            .map(applyScaleFactorsL8L9)
            .select(BANDS, NEW_BAND_NAMES)
            .map(getFractions)
            .map(getNdfi)
        )

        col9 = (ee.ImageCollection('LANDSAT/LC09/C02/T1_L2')
            .filterBounds(center.geometry())
            .filterDate(y + '-01-01', y + '-12-30')
            .filter('CLOUD_COVER <= 50')
            .map(lambda image: image.set('sensor', 'L8'))
            .map(applyScaleFactorsL8L9)
            .select(BANDS, NEW_BAND_NAMES)
            .map(getFractions)
            .map(getNdfi)
        )

        collection = col8.merge(col9).map(removeCloudShadow)

        listIdScene = collection.reduceColumns(ee.Reducer.toList(), ['LANDSAT_SCENE_ID']).get('list').getInfo()

        for idScene in listIdScene:

            currentScene = ee.Image(collection.filter(ee.Filter.eq('LANDSAT_SCENE_ID', idScene)).first())

            sampleValues = currentScene.sampleRegions(
                collection=samplesDataset,  
                scale=30, 
                geometries=True
            )

            sampleValues = sampleValues\
                .map(lambda feat: feat.set('LANDSAT_SCENE_ID', idScene))\
                .map(lambda feat: feat.set('PR', pr))\
                .map(lambda feat: feat.set('YEAR', y))


            listSceneData.append(sampleValues)

    datasetSampleValues = ee.FeatureCollection(listSceneData).flatten()

    return datasetSampleValues


'''
    trigger function main
'''

def run():


    mapPr = map(lambda pr: extractSamplesByPr(pr), TEST_PR)

    dataset = ee.FeatureCollection(list(mapPr)).flatten()

    '''
        Export data
    '''
    descriptionTest = 'lapig_samples_w_edge_amazonia_v1_test1'

    task = ee.batch.Export.table.toDrive(
        description=descriptionTest,
        collection=dataset,
        fileFormat='CSV',
        selectors=SELECTORS,
        folder='MAPBIOMAS_EXPORTS'
    )

    task.start()



if __name__ == '__main__':
    run()