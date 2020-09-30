
import sys
import json
# import cPickle as pkl
import pickle as pkl

import pymongo
import bson
import gridfs

import mongo_utils

import config
from schema import type_utils



#
#
#
class VehicleFlags(object):

    def __init__(self):
        """ Constructor sets flag defaults.
        """

        self.targetHasTransform      = False



#
# -- AhnungVehicle
#
# Each estimator configured in the AhnungConfig is represented/implemented by a
# separate instance of AhnungVehicle.  The vehicle (eventually) contains all metadata and
# state required to implement the predictor.  This includes schema information,
# attribute mappings and the autosklearn ensemble fitted model.
#
class AhnungVehicle(object):
    """ Global container object for an estimator.
        Holds the AhnungConfig, settings, flags, conversions, model ensemble needed to
        implement an Ahnung estimator.
    """

    ATTR_OBJ_NAME       = 'objectName'
    ATTR_GRIDFS_ID      = 'gridFSId'
    
    FS_NAME_ACONFIG     = 'aconfig'
    FS_NAME_FLAGS       = 'flags'
    FS_NAME_TRANSFORMS  = 'transforms'
    FS_NAME_CLASSIFIER  = 'classifier'
    FS_NAME_REGRESSOR   = 'regressor'

    fsNameList = [FS_NAME_FLAGS, FS_NAME_TRANSFORMS, FS_NAME_CLASSIFIER, FS_NAME_REGRESSOR]

    #
    #
    #
    def __init__(self, estName, target, aConfig):

        self.estName                = estName          # Name of estimator/dataset
        self.target                 = target           # Attribute to estimate/predict
        self.aConfig                = aConfig          # Static/global configuration
        self.flags                  = VehicleFlags()   # Dynamic flags for this estimator
        self.mUtils                 = None
        self.metaClientDB           = None
        self.fsIds                  = None
        self.gridFS                 = None
        self.attrDefaults           = None
        self.attrDatatypes          = None
        self.attrSenses             = None
        self.attrStats              = None
        self.rejectAttrs            = None
        self.attrTransformDict      = None
        self.autoSklearnClassifier  = None
        self.autoSklearnRegressor   = None




    #
    #
    #
    def getEstimatorName(self):
        return self.estName


    #
    #
    #
    def getEstimatorTarget(self):
        return self.target


    #
    #
    #
    def getAhnungConfig(self):
        return self.aConfig


    #
    #
    #
    def getIsRegression(self):
        estName = self.getEstimatorName()
        isRegressionBool = self.aConfig.getEstimatorBooleanFlag(estName, self.aConfig.IS_REGRESSION)
        return isRegressionBool


    #
    #
    #
    def getIsClassification(self):
        estName = self.getEstimatorName()
        isClassificationBool = self.aConfig.getEstimatorBooleanFlag(estName, self.aConfig.IS_CLASSIFICATION)
        return isClassificationBool



    #
    #
    #
    def getRandomSeed(self):

        c_rand_seed = self.aConfig.getRandomSeed()
        estName = self.getEstimatorName()
        rand_seed = self.aConfig.getEstimatorInteger(estName, self.aConfig.RANDOM_SEED, c_rand_seed)
        
        return rand_seed



    #
    #
    #
    def getAllowedCPUs(self):

        c_allow_cpus = self.aConfig.getAllowedCPUs()
        estName = self.getEstimatorName()
        allow_cpus = self.aConfig.getEstimatorInteger(estName, self.aConfig.ALLOWED_CPUS, c_allow_cpus)

        return allow_cpus



    #
    #
    #
    def getMaxGlobalTime(self):

        c_max_global = self.aConfig.getMaxGlobalTime()
        estName = self.getEstimatorName()
        max_global = self.aConfig.getEstimatorInteger(estName, self.aConfig.MAX_GLOBAL_TIME, c_max_global)
        
        return max_global



    #
    #
    #
    def getMaxPerModelTime(self):

        c_max_permodel = self.aConfig.getMaxPerModelTime()
        estName = self.getEstimatorName()
        max_permodel = self.aConfig.getEstimatorInteger(estName, self.aConfig.MAX_PERMODEL_TIME, c_max_permodel)
        
        return max_permodel


    #
    #
    #
    def getEnsembleSize(self):

        c_ensemble_size = self.aConfig.getEnsembleSize()
        estName = self.getEstimatorName()
        ensemble_size = self.aConfig.getEstimatorInteger(estName, self.aConfig.ENSEMBLE_SIZE, c_ensemble_size)
        
        return ensemble_size


    #
    #
    #
    def getEnsembleNBest(self):

        c_ensemble_nbest = self.aConfig.getEnsembleNBest()
        estName = self.getEstimatorName()
        ensemble_nbest_int = self.aConfig.getEstimatorInteger(estName, self.aConfig.ENSEMBLE_NBEST, -1)
        if 0 < ensemble_nbest_int:
            ensemble_nbest = ensemble_nbest_int
        else:
            ensemble_nbest_float = self.aConfig.getEstimatorFloat(estName, self.aConfig.ENSEMBLE_NBEST, -1.0)
            if 0.0 < ensemble_nbest_float:
                ensemble_nbest = ensemble_nbest_float
            else:
                ensemble_nbest = c_ensemble_nbest
        
        return ensemble_nbest


    #
    #
    #
    def getMaxModelsOnDisc(self):

        c_max_models_on_disc = self.aConfig.getMaxModelsOnDisc()
        estName = self.getEstimatorName()
        max_models_on_disc = self.aConfig.getEstimatorInteger(estName, self.aConfig.MAX_MODELS_ON_DISC, c_max_models_on_disc)
        
        return max_models_on_disc


    #
    #
    #
    def getMetric(self):

        c_metric = self.aConfig.getMetric()
        estName = self.getEstimatorName()
        metric = self.aConfig.getEstimatorString(estName, self.aConfig.METRIC, c_metric)
        
        return metric



    #
    #
    #
    def getMongoUtils(self):
        if None == self.mUtils:
            self.mUtils = mongo_utils.MongoUtils()
            
        return self.mUtils


    #
    #
    #
    def getMetaClientDB(self):

        if None == self.metaClientDB:
            # Obtain client connection to the metadata collections database.
            # This is the storage location for metadata detected by the schema stage.
            meta_uri = self.aConfig.getMetaDataURI()
            mUtils = self.getMongoUtils()
            metaClient = mUtils.getMongoClient(meta_uri)
            self.metaClientDB = metaClient.get_default_database()
            
        return self.metaClientDB


    #
    #
    #
    def loadFSIds(self):
        
        newFSIds = {}
        
        fsCollStr = self.getEstimatorName() + type_utils.FSIDS_SUFFIX
        metaClientDB = self.getMetaClientDB()
        fsColl = pymongo.collection.Collection( metaClientDB, fsCollStr )
        
        for fName in self.fsNameList:
            newFSIds[fName] = None
            
        fsQuery = {}
        for nfs in fsColl.find( fsQuery ):
    
            docKeys = nfs.keys()
            if self.ATTR_OBJ_NAME in docKeys and self.ATTR_GRIDFS_ID in docKeys:
                fName = nfs[self.ATTR_OBJ_NAME]
                fId   = nfs[self.ATTR_GRIDFS_ID]
                newFSIds[fName] = fId

        self.fsIds = newFSIds
        
        return self.fsIds


    #
    #
    #
    def saveFSIds(self):
        
        if None != self.fsIds:
        
            fsCollStr = self.getEstimatorName() + type_utils.FSIDS_SUFFIX
            metaClientDB = self.getMetaClientDB()
            fsColl = pymongo.collection.Collection( metaClientDB, fsCollStr )
            fsColl.drop()
            
            idDict = self.fsIds
            if None != idDict:
                fNameList = idDict.keys()
                for fName in fNameList:
                    fsId = idDict[fName]
                    fsColl.insert({ self.ATTR_OBJ_NAME: fName, self.ATTR_GRIDFS_ID: fsId })


    #
    #
    #
    def getFSId(self, filename, doLoad=False):
        
        if None == self.fsIds or doLoad:
            self.fsIds = self.loadFSIds()

        resId = None
        if None != filename:
            if filename in self.fsIds.keys():
                resId = self.fsIds[filename]          
        
        return resId


    #
    #
    #
    def setFSId(self, filename, fsId, doFlush=False):
        
        if None == self.fsIds:
            self.fsIds = self.loadFSIds()

        if None != filename:
            self.fsIds[filename] = fsId
        
        if doFlush:
            self.saveFSIds()


    #
    #
    #
    def getGridFS(self):
        
        if None == self.gridFS:
            eName = self.getEstimatorName()
            metaClientDB = self.getMetaClientDB()
            self.gridFS = gridfs.GridFS(metaClientDB, collection=eName)            
            
        return self.gridFS


    #
    #
    #
    def loadVehicleObject(self, objectName):

        resObj = None
        
        gfs = self.getGridFS()
        oid = self.getFSId(objectName)
        
        if None != oid:
            try:
                fds = gfs.get(oid)
                resObj = pkl.load(fds)
            finally:
                fds.close()
        
        return resObj


    #
    #
    #
    def saveVehicleObject(self, objectName, objVal):
        
        gfs = self.getGridFS()
        oid = self.getFSId(objectName)

        if None != oid:
            gfs.delete(oid)

        newF_id = gfs.put(pkl.dumps(objVal, pkl.HIGHEST_PROTOCOL), filename=objectName)
        self.setFSId(objectName, newF_id)


    #
    #
    #
    def getAttrDefaults(self, doLoad=False):

        if None == self.attrDefaults or doLoad:
            defaultsDict = {}

            dCollStr = self.getEstimatorName() + type_utils.DEFAULTS_SUFFIX
            metaClientDB = self.getMetaClientDB()
            defaultsColl = pymongo.collection.Collection( metaClientDB, dCollStr )
        
            defaultsQuery = {}
            for nDefault in defaultsColl.find( defaultsQuery ):
        
                for key, value in nDefault.items():
        
                    if None != key and None != value:
                        if '_id' != key:
                            defaultsDict[key] = value

            self.attrDefaults = defaultsDict

        return self.attrDefaults


    #
    #
    #
    def setAttrDefaults(self, aDefaults, doFlush=False):
        self.attrDefaults = aDefaults
        
        if doFlush:
            
            dCollStr = self.getEstimatorName() + type_utils.DEFAULTS_SUFFIX
            metaClientDB = self.getMetaClientDB()
            defaultsColl = pymongo.collection.Collection( metaClientDB, dCollStr )
            defaultsColl.drop()
            
            if None != aDefaults:
                pathList = aDefaults.keys()
                for path in pathList:
                    attrDefault = aDefaults[path]
                    defaultsColl.insert({ path: attrDefault })



    #
    #
    #
    def getAttrDatatypes(self, doLoad=False):

        if None == self.attrDatatypes or doLoad:
            typesDict = {}

            tCollStr = self.getEstimatorName() + type_utils.TYPES_SUFFIX
            metaClientDB = self.getMetaClientDB()
            typesColl = pymongo.collection.Collection( metaClientDB, tCollStr )
        
            typesQuery = {}
            for nType in typesColl.find( typesQuery ):
        
                for key, value in nType.items():
        
                    if None != key and None != value:
                        if '_id' != key:
                            typesDict[key] = value

            self.attrDatatypes = typesDict
            
        return self.attrDatatypes


    #
    #
    #
    def setAttrDatatypes(self, aDatatypes, doFlush=False):
        self.attrDatatypes = aDatatypes
        
        if doFlush:
            
            tCollStr = self.getEstimatorName() + type_utils.TYPES_SUFFIX
            metaClientDB = self.getMetaClientDB()
            typesColl = pymongo.collection.Collection( metaClientDB, tCollStr )
            typesColl.drop()
            
            if None != aDatatypes:
                pathList = aDatatypes.keys()
                for path in pathList:
                    attrType = aDatatypes[path]
                    typesColl.insert({ path: attrType })


    #
    #
    #
    def getAttrSenses(self, doLoad=False):

        if None == self.attrSenses or doLoad:
            sensesDict = {}

            fCollStr = self.getEstimatorName() + type_utils.SENSES_SUFFIX
            metaClientDB = self.getMetaClientDB()
            sensesColl = pymongo.collection.Collection( metaClientDB, fCollStr )

            sensesQuery = {}
            for nSense in sensesColl.find( sensesQuery ):
        
                for key, value in nSense.items():
        
                    if None != key and None != value:
                        if '_id' != key:
                            sensesDict[key] = value

            self.attrSenses = sensesDict
            
        return self.attrSenses


    #
    #
    #
    def setAttrSenses(self, aSenses, doFlush=False):
        self.attrSenses = aSenses

        if doFlush:
            
            fCollStr = self.getEstimatorName() + type_utils.SENSES_SUFFIX
            metaClientDB = self.getMetaClientDB()
            sensesColl = pymongo.collection.Collection( metaClientDB, fCollStr )
            sensesColl.drop()
            
            if None != aSenses:
                pathList = aSenses.keys()
                for path in pathList:
                    attrType = aSenses[path]
                    sensesColl.insert({ path: attrType })


    #
    #
    #
    def getAttrStats(self, doLoad=False):

        if None == self.attrStats or doLoad:
            statsDict = {}

            sCollStr = self.getEstimatorName() + type_utils.STATS_SUFFIX
            metaClientDB = self.getMetaClientDB()
            sAttrsColl = pymongo.collection.Collection( metaClientDB, sCollStr )

            statsQuery = {}
            for nStats in sAttrsColl.find( statsQuery ):
        
                for key, value in nStats.items():
        
                    if None != key and None != value:
                        if '_id' != key:
                            statsDict[key] = value

            self.attrStats = statsDict
            
        return self.attrStats


    #
    #
    #
    def setAttrStats(self, aStats, doFlush=False):
        self.attrStats = aStats

        if doFlush:
            
            sCollStr = self.getEstimatorName() + type_utils.STATS_SUFFIX
            metaClientDB = self.getMetaClientDB()
            sAttrsColl = pymongo.collection.Collection( metaClientDB, sCollStr )
            sAttrsColl.drop()
            
            if None != aStats:
                pathList = aStats.keys()
                for path in pathList:
                    attrData = aStats[path]
                    sAttrsColl.insert({ path: attrData })


    #
    #
    #
    def getRejectedAttrs(self, doLoad=False):

        if None == self.rejectAttrs or doLoad:
            rejectDict = {}

            rCollStr = self.getEstimatorName() + type_utils.REJECT_SUFFIX
            metaClientDB = self.getMetaClientDB()
            rAttrsColl = pymongo.collection.Collection( metaClientDB, rCollStr )

            rejectQuery = {}
            for nReject in rAttrsColl.find( rejectQuery ):
        
                for key, value in nReject.items():
        
                    if None != key and None != value:
                        if '_id' != key:
                            rejectDict[key] = value

            self.rejectAttrs = rejectDict
            
        return self.rejectAttrs


    #
    #
    #
    def setRejectedAttrs(self, rAttrs, doFlush=False):
        self.rejectAttrs = rAttrs

        if doFlush:
            
            rCollStr = self.getEstimatorName() + type_utils.REJECT_SUFFIX
            metaClientDB = self.getMetaClientDB()
            rAttrsColl = pymongo.collection.Collection( metaClientDB, rCollStr )
            rAttrsColl.drop()
            
            if None != rAttrs:
                pathList = rAttrs.keys()
                for path in pathList:
                    attrData = rAttrs[path]
                    rAttrsColl.insert({ path: attrData })


    #
    #
    #
    def getAttrTransform(self, attrName, doLoad=False):

        if None == self.attrTransformDict or doLoad:
            self.attrTransformDict = {}
            
            loaded = self.loadVehicleObject(self.FS_NAME_TRANSFORMS)
            
            if None != loaded:
                self.attrTransformDict = loaded

        aTransform = None
        if None != attrName:
            aTransform = self.attrTransformDict[attrName]
            
        return aTransform


    #
    #
    #
    def setAttrTransform(self, attrName, targetEncoder, doFlush=False):
        
        if None == self.attrTransformDict:
            self.attrTransformDict = {}
        
        if None != attrName:
            self.attrTransformDict[attrName] = targetEncoder
        
        if doFlush:
            self.saveVehicleObject(self.FS_NAME_TRANSFORMS, self.attrTransformDict)



    #
    #
    #
    def getAutoSklearnClassifier(self, doLoad=False):

        if None == self.autoSklearnClassifier or doLoad:
            self.autoSklearnClassifier = None
            
            loaded = self.loadVehicleObject(self.FS_NAME_CLASSIFIER)
            
            if None != loaded:
                self.autoSklearnClassifier = loaded
            
        return self.autoSklearnClassifier



    #
    #
    #
    def setAutoSklearnClassifier(self, modelEnsemble, doFlush=False):
        
        self.autoSklearnClassifier = modelEnsemble
        
        if doFlush:
            self.saveVehicleObject(self.FS_NAME_CLASSIFIER, self.autoSklearnClassifier)



    #
    #
    #
    def getAutoSklearnRegressor(self, doLoad=False):

        if None == self.autoSklearnRegressor or doLoad:
            self.autoSklearnRegressor = None
            
            loaded = self.loadVehicleObject(self.FS_NAME_REGRESSOR)
            
            if None != loaded:
                self.autoSklearnRegressor = loaded
            
        return self.autoSklearnRegressor



    #
    #
    #
    def setAutoSklearnRegressor(self, modelEnsemble, doFlush=False):
        
        self.autoSklearnRegressor = modelEnsemble
        
        if doFlush:
            self.saveVehicleObject(self.FS_NAME_REGRESSOR, self.autoSklearnRegressor)



    #
    #
    #
    def doFlushAll(self):

        # if None != self.aConfig:
        #     self.saveVehicleObject(self.FS_NAME_ACONFIG, self.aConfig)

        if None != self.attrDefaults:
            self.setAttrDefaults(self.attrDefaults, True)
        
        if None != self.attrDatatypes:
            self.setAttrDatatypes(self.attrDatatypes, True)
        
        if None != self.attrSenses:
            self.setAttrSenses(self.attrSenses, True)
        
        if None != self.attrStats:
            self.setAttrStats(self.attrStats, True)

        if None != self.rejectAttrs:
            self.setRejectedAttrs(self.rejectAttrs, True)

        if None != self.attrTransformDict:
            self.setAttrTransform(None, None, True)

        if None != self.autoSklearnClassifier:
            self.setAutoSklearnClassifier(self.autoSklearnClassifier, True)

        if None != self.autoSklearnRegressor:
            self.setAutoSklearnRegressor(self.autoSklearnRegressor, True)

        if None != self.fsIds:
            self.saveFSIds()



    #
    # Preload the classifier/regressor model from the MongoDB storage layer.
    #
    def doLoadModel(self):

        if self.getIsClassification():
            self.getAutoSklearnClassifier()

        if self.getIsRegression():
            self.getAutoSklearnRegressor()


    #
    # Preload the classifier/regressor model from the MongoDB storage layer.
    #
    def getAutoSKLearnModel(self):

        model = None
        
        if self.getIsClassification():
            model = self.getAutoSklearnClassifier()

        if self.getIsRegression():
            model = self.getAutoSklearnRegressor()

        return model


    #
    #
    #
    def getTargetLabels(self):
        
        resultLabels = None
        targetName = self.getEstimatorTarget()
        targetTransform = self.getAttrTransform(targetName)
        if None != targetTransform:
            resultLabels = list(targetTransform.classes_)
        
        return resultLabels



