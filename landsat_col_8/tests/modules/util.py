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