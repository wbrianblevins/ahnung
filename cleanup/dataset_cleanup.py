#!/usr/bin/env python3

import sys
import json
from datetime import datetime as dt

import pandas as pd
import pymongo
import bson

import config
import vehicle
import mongo_utils

from schema import schema_analysis
from schema import type_utils


#
#
#
def normalizeToList(nFlat, pathList, valTypes, defValues, target):

    failDoc   = False
    normDoc   = {}
    valList   = []
    
    defAllow  = 1 + (len(pathList) // 10)
    defCount  = 0
    
    for key in pathList:
        
        defValue = defValues[key]
        requiredType = valTypes[key]
        value = nFlat.get(key)
        
        if None == value:
            if target == key:
                # The target attribute must have a value present.
                failDoc = True
                break
            else:
                # Count the number of times the default value was used.
                normValue   = defValue
                defCount   += 1
        else:
            # Check the type of value.
            fkType, fkValue = type_utils.ahnungTypeAndValue(value)
            # If this attribute value does not match the normalized type,
            # convert to the best value or use the default.
            if type_utils.TYPE_UNKNOWN == fkType:
                failDoc = True
                break
            elif requiredType != fkType:
                if type_utils.TYPE_INT == requiredType:
                    normValue, defCount = type_utils.convert_int(value, defValue, defCount)
                elif type_utils.TYPE_FLOAT == requiredType:
                    normValue, defCount = type_utils.convert_float(value, defValue, defCount)
                elif type_utils.TYPE_DATE == requiredType:
                    normValue, defCount = type_utils.convert_date(value, defValue, defCount)
                elif type_utils.TYPE_STRING == requiredType:
                    normValue, defCount = type_utils.convert_string(value, defValue, defCount)
            else:
                normValue = fkValue
        
        normDoc[key] = normValue
        valList.append(normValue)

    # If too many default values were required, reject the instance/document.    
    if defCount > defAllow:
        failDoc = True

    resultDoc  = None
    resultList = None
    if not failDoc:
        resultDoc  = normDoc
        resultList = valList
        
    return resultDoc, resultList






class CleanupStage(object):
    """ The Ahnung cleanup stage pulls flattened instances (in documents,
        aka rows) from the raw document source collection.  The values in
        each instance are cleaned and normalized so that there are no
        missing values.  Additional cleaning may include outlier suppression
        and normalization to a standard type for each attribute.
    """

    #
    # Dictionary keys (constants) used in the schema analysis.
    #
    PRESENT_COUNT   = type_utils.PRESENT_COUNT
    ATTR_TYPES      = type_utils.ATTR_TYPES
    ATTR_VALUES     = type_utils.ATTR_VALUES

    #
    # Attribute path separator character.
    #
    SA_SEPARATOR    = type_utils.SA_SEPARATOR

    #
    # Names of types tracked in the schema analysis.
    #
    TYPE_INT        = type_utils.TYPE_INT
    TYPE_LONG       = type_utils.TYPE_LONG
    TYPE_FLOAT      = type_utils.TYPE_FLOAT
    TYPE_STRING     = type_utils.TYPE_STRING
    TYPE_DATE       = type_utils.TYPE_DATE

    #
    # Meta data collection name suffixes
    #
    TYPES_SUFFIX    = type_utils.TYPES_SUFFIX
    DEFAULTS_SUFFIX = type_utils.DEFAULTS_SUFFIX

    #
    #
    #
    def __init__(self, aConfig):

        self.aConfig = aConfig



    #
    # Cleanup the dataset of the documents/rows in `collName`.
    # 
    #
    def cleanupEst(self, rawClientDB, vehicle, cleanedClientDB, target, collName):

        print('\nCleanup for dataset ' + collName + ' ...\n')

        valTypes   = vehicle.getAttrDatatypes()
        defValues  = vehicle.getAttrDefaults()
        
        pathList = valTypes.keys()
        
        destColl = pymongo.collection.Collection( cleanedClientDB, collName )
        destColl.drop()

        srcColl   = pymongo.collection.Collection( rawClientDB, collName )
        srcQuery  = {  target : { "$exists": True }}

        print('For estimator ' + collName)
        print('Types:')
        print(str(valTypes))

        dsList = []
        
        for nFlat in srcColl.find( srcQuery ):

            normDoc, valList = normalizeToList(nFlat, pathList, valTypes, defValues, target)
            if None != normDoc and None != valList:
                destColl.insert(normDoc)
                dsList.append(valList)
            
        dsFrame = pd.DataFrame(dsList, columns=pathList)

        print('DataFrame:')
        print(str(dsFrame))



    #
    #
    #
    def cleanup(self):

        mUtils = mongo_utils.MongoUtils()

        # Obtain client connection to the raw documents collections database.
        # This is the source for the cleanup stage.
        raw_uri = self.aConfig.getRawDocsURI()
        rawClient = mUtils.getMongoClient(raw_uri)
        rawClientDB = rawClient.get_default_database()

        # Obtain client connection to the metadata collections database.
        # This information is used to refine the cleanup.
        # meta_uri = self.aConfig.getMetaDataURI()
        # metaClient = mUtils.getMongoClient(meta_uri)
        # metaClientDB = metaClient.get_default_database()

        # Obtain client connection to the folds collections database.
        # This is the destination for the cleanup stage.
        # Type normalized documentes with missing attributes filled in are
        # stored here for use by subsequent stages.
        cleaned_uri = self.aConfig.getCleanedURI()
        cleanedClient = mUtils.getMongoClient(cleaned_uri)
        cleanedClientDB = cleanedClient.get_default_database()

        # print(rawClientDB)
        # print(metaClientDB)

        print('\n=============================================')
        print('\tCLEANUP STAGE...')
        print('=============================================\n')

        # Get the list of estimators (predictors) that will be constructed.
        estList = self.aConfig.getEstimatorList()

        # Build the schema and transfer raw docs for each estimator.
        for estimator in estList:
            collName = estimator.get(self.aConfig.SRC_COLLNAME)
            target = estimator.get(self.aConfig.TARGET_NAME)
            vehicle = self.aConfig.getEstVehicle(collName)
            self.cleanupEst(rawClientDB, vehicle, cleanedClientDB, target, collName)
            vehicle.doFlushAll()



""" When launched as a script, load the configuration settings and run
    the dataset cleanup stage.
"""
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('Specify the configuration settings as the first and only parameter.')
        sys.exit()

    csFname = sys.argv[1]
    confSettings = config.AhnungConfig(csFname)

    cStage = CleanupStage(confSettings)

    print(confSettings.aConfig)

    cStage.clenaup()
    
    

