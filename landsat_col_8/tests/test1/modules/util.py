import ee
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

    # list of random ids
    randomIdList = ee.List(
        collection.reduceColumns(ee.Reducer.toList(), ['new_id'])
        .get('list'))

    # list of sequential ids
    sequentialIdList = ee.List.sequence(1, collection.size())

    # set new ids
    shuffled = collection.remap(randomIdList, sequentialIdList, 'new_id')

    return shuffled

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
