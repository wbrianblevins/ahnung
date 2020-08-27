#!/usr/bin/env python3

import sys
import json
import time
# from datetime import datetime as dt

import pandas as pd
import pymongo
# import bson
import numpy

import config
# import vehicle
import mongo_utils

import autosklearn.classification
import autosklearn.metrics
import sklearn.model_selection
import sklearn.datasets
import sklearn.metrics
import sklearn.utils
import sklearn.inspection

# from schema import schema_analysis
from schema import type_utils

from cleanup import dataset_cleanup


#
# Select included/excluded estimators and preprocessors.
#
# use_est      = ["random_forest", "liblinear_svc"]
use_est      = None
exc_est      = None
# use_preproc  = ["no_preprocessing", ]
use_preproc  = None
exc_preproc  = None

#
# How many CPUs should be used at the same time?
#
allowed_jobs = 4

#
# Select overall max time in seconds and per model max time in seconds.
#
# time_left_for_this_taskint, optional (default=3600)
max_time_global = 360
# per_run_time_limitint, optional (default=360)
max_time_model = 36



class ExplorationStage(object):
    """ The Ahnung exploration stage pulls normalized and standardized
        instances (in documents, aka rows) from the cleaned document source
        collection.  The documents are combined into a Pandas DataFrame
        and then passed to autosklearn to search for accurate model and
        hyperparameter configurations.
    """


    #
    #
    #
    def __init__(self, aConfig):

        self.aConfig = aConfig



    #
    # Explore models and hyperparameters for the dataset of the
    # documents/rows in `collName`.
    # 
    #
    def loadCleanDF(self, srcColl, srcQuery, estVehicle, target):

        valTypes   = estVehicle.getAttrDatatypes()
        defValues  = estVehicle.getAttrDefaults()
        attrSenses = estVehicle.getAttrSenses()
        
        pathList   = valTypes.keys()

        print('For estimator ' + estVehicle.getEstimatorName())
        print('\tTypes selected: ' + str(valTypes))
        print('\tSenses selected: ' + str(attrSenses))
        
        dsList = []
        
        for nFlat in srcColl.find( srcQuery ):

            valList, normDoc = dataset_cleanup.normalizeToList(nFlat, pathList, valTypes, defValues, target)
            if None != valList:
                dsList.append(valList)

        dsFrame = pd.DataFrame(dsList, columns=pathList)

        # print('DataFrame:')
        # print(str(dsFrame))
        
        return dsFrame


    #
    #
    #
    def generateSenseList(self, attrNames, senseDict):
        senseList = []

        for nAttr in attrNames:
            senseList.append(senseDict[nAttr])

        return senseList


    #
    #
    #
    def splitDatasetXandY(self, cleanDF, target):
        
        all_y = cleanDF[target].copy()
        all_X = cleanDF.drop(target, 1)
        
        return all_X, all_y



    #
    #
    #
    def balanceSamples(self, X_train, y_train, estVehicle):
        
        target   = estVehicle.getEstimatorTarget()

        balFlag  = self.aConfig.getModelCategoryBalancing()
        maxOver  = self.aConfig.getModelMaxOversample()

        if self.aConfig.BALANCE_CAT_NONE == balFlag:
            return X_train.copy(), y_train.copy()

        # combined = numpy.column_stack((X_train, y_train))
        combined = X_train.copy()
        combined[target] = y_train
        
        uniq, unq_idx, unq_cnt = numpy.unique(y_train, return_inverse=True, return_counts=True)
        catCnt                 = len(uniq)
        inputCnt               = combined.shape[0]

        unq_wts                = numpy.zeros(catCnt)
        if self.aConfig.BALANCE_CAT_EQUAL == balFlag:
            unq_wts[:] = 1 / catCnt
        else:
            # Assume balFlag == self.aConfig.BALANCE_CAT_AVG
            eFrac = 1 / catCnt
            for idx in range(catCnt):
                unq_wts[idx] = (eFrac + (unq_cnt[idx] / inputCnt)) / 2
            
        min_cnt_idx  = numpy.argmin(unq_cnt)
        min_wts_idx  = numpy.argmin(unq_wts)
        
        max_allowed_count = ( maxOver * unq_cnt[min_cnt_idx] ) / unq_wts[min_wts_idx]
        
        target_count = min(max_allowed_count, inputCnt)
        
        resCatCnt    = numpy.array((len(uniq),), dtype=numpy.int32)
        resCatCnt    = (unq_wts * target_count).astype(numpy.int32)

        row_sets     = []
        balidx       = 0

        for idx in range(len(uniq)):
    
            cat_cnt = unq_cnt[idx]
            cat_remain = resCatCnt[idx]
            while cat_remain >= cat_cnt:
                indices     = numpy.where(unq_idx == idx)[0]
                row_sets.append(combined.iloc[indices,:])
                cat_remain  = cat_remain - cat_cnt
                balidx      = balidx + cat_cnt
                
            if cat_remain > 0:
                indices     = numpy.random.choice(numpy.where(unq_idx == idx)[0], cat_remain)
                row_sets.append(combined.iloc[indices,:])
                balidx      = balidx + cat_remain
        
        balanced     = pd.concat(row_sets, ignore_index=True)
        balanced     = sklearn.utils.shuffle(balanced)

        y_res        = balanced[target].copy()
        del balanced[target]
        X_res        = balanced.copy()

        return X_res, y_res


    #
    #
    #
    def computeStats(self, automl, target, X_test, y_test, estVehicle):

        estName       = estVehicle.getEstimatorName()
        rand_seed     = estVehicle.getRandomSeed()
        allowed_jobs  = estVehicle.getAllowedCPUs()


        ## print('\nEXPLORE, y_test = ' + str(y_test))
        ## print('EXPLORE, X_test = ' + str(X_test))
        np_x_test     = X_test.to_numpy(dtype='float')
        np_y_test     = y_test.to_numpy(dtype='float')
        ## print('EXPLORE, X_test.dtype = ' + str(np_x_test.dtype))
        print("\nChecking the AutoSklearnClassifier against the " + estName + " test dataset.")
        y_hat         = automl.predict(np_x_test, batch_size=None, n_jobs=1)
     
        print("\n\tAccuracy score: ", sklearn.metrics.accuracy_score(y_test, y_hat))
        
        #
        # Record the held-out, test data true values and predicted values.
        #
        allStats      = estVehicle.getAttrStats()
        targetEncoder = estVehicle.getAttrTransform(target)
        allStats[type_utils.STATS_Y_TESTING] = targetEncoder.inverse_transform(y_test).tolist()
        allStats[type_utils.STATS_Y_PREDICT] = targetEncoder.inverse_transform(y_hat).tolist()

        #
        # Compute and store Precision and Recall for each label.
        #
        if estVehicle.getIsClassification():

            precisionDict = {}
            recallDict    = {}

            labelList       = estVehicle.getTargetLabels()
            internalLabels  = targetEncoder.transform(labelList)
            recallStats     = sklearn.metrics.recall_score(np_y_test, y_hat, labels=internalLabels, average=None)
            precisionStats  = sklearn.metrics.precision_score(np_y_test, y_hat, labels=internalLabels, average=None)
            idx             = 0
            for targetClass in labelList:
                precisionDict[targetClass]  = precisionStats[idx]
                recallDict[targetClass]     = recallStats[idx]
                idx                        += 1

            allStats[type_utils.STATS_PRECISION_SCORE] = precisionDict
            allStats[type_utils.STATS_RECALL_SCORE]    = recallDict

        #
        # Compute ROC curves for each target class/label.
        #
        if estVehicle.getIsClassification():
            proba_y_hat = automl.predict_proba(np_x_test, batch_size=None, n_jobs=1)

            fprDict         = {}
            tprDict         = {}
            rocaucDict      = {}
            labelList       = estVehicle.getTargetLabels()
            internalLabels  = targetEncoder.transform(labelList)
            y_test_plabel   = numpy.zeros((len(np_y_test), len(internalLabels)))
            for idx in internalLabels:
                y_test_plabel[np_y_test == idx,idx] = 1
            # rocaucStats     = sklearn.metrics.roc_auc_score(y_true=np_y_test, y_score=proba_y_hat, labels=internalLabels, average=None, multi_class='ovr')
            idx             = 0
            for targetClass in labelList:
                # rocaucDict[targetClass]  = rocaucStats[idx]
                rocaucDict[targetClass]  = sklearn.metrics.roc_auc_score(y_true=y_test_plabel[:,idx], y_score=proba_y_hat[:,idx])
                tcIdx                    = targetEncoder.transform(numpy.array([targetClass]))[0]
                fpr, tpr, thresholds     = sklearn.metrics.roc_curve(y_test, proba_y_hat[:,tcIdx], pos_label=tcIdx)
                fprDict[str(tcIdx)]      = fpr.tolist()
                tprDict[str(tcIdx)]      = tpr.tolist()
                idx                     += 1

            allStats[type_utils.STATS_ROCAUC_SCORE]  = rocaucDict
            allStats[type_utils.STATS_FPR]           = fprDict
            allStats[type_utils.STATS_TPR]           = tprDict

        #
        # Compute Permutation Importance
        #
        p_imp = sklearn.inspection.permutation_importance(automl, np_x_test, np_y_test, n_repeats=10, random_state=rand_seed, n_jobs=allowed_jobs)
        allStats[type_utils.STATS_ATTR_NAMES]  = list(X_test)
        allStats[type_utils.STATS_PERMI_MEAN]  = p_imp.importances_mean.tolist()
        allStats[type_utils.STATS_PERMI_STD]   = p_imp.importances_std.tolist()
        allStats[type_utils.STATS_PERMI_VALS]  = p_imp.importances.tolist()

        estVehicle.setAttrStats(allStats)
        
        return y_hat


    #
    # Explore models and hyperparameters for the dataset of the
    # documents/rows in `estName`.
    # 
    #
    def exploreSKLearn(self, target, estVehicle, cleanDF):

        estName = estVehicle.getEstimatorName()
        
        rand_seed = estVehicle.getRandomSeed()

        start_wc_seconds = time.time()
        
        #
        # Select included/excluded estimators and preprocessors.
        #
        # use_est      = ["random_forest", "liblinear_svc"]
        use_est      = None
        exc_est      = None
        # use_preproc  = ["no_preprocessing", ]
        use_preproc  = None
        exc_preproc  = None
        
        #
        # How many CPUs should be used at the same time?
        #
        allowed_jobs = estVehicle.getAllowedCPUs()
        
        #
        # Select overall max time in seconds and per model max time in seconds.
        #
        # time_left_for_this_taskint, optional (default=3600)
        max_time_global = estVehicle.getMaxGlobalTime()
        # per_run_time_limitint, optional (default=360)
        max_time_model = estVehicle.getMaxPerModelTime()
        
        print("Selected estimators      : ", use_est)
        print("Deselected estimators    : ", exc_est)
        print("Selected preprocessors   : ", use_preproc)
        print("Deselected preprocessors : ", exc_preproc)
        print("Max time global (sec)    : ", max_time_global)
        print("Max time per model (sec) : ", max_time_model)
        print("CPUs Used                : ", allowed_jobs)
        
        print("Using " + estName + " dataset.")
        
        all_X, all_y = self.splitDatasetXandY(cleanDF, target)
        
        print("Splitting out test and train datasets.")
        X_train, X_test, y_train, y_test = \
                sklearn.model_selection.train_test_split(all_X, all_y, test_size=0.2, random_state=rand_seed)
        
        
        # autosklearn.regression.AutoSklearnRegressor

        # automl = estVehicle.getAutoSklearnClassifier()
        # if None == automl:

        print("Instantiating AutoSklearnClassifier.")
        strategy_args = {'folds': 5}
        automl = autosklearn.classification.AutoSklearnClassifier(
                     time_left_for_this_task = max_time_global,
                     per_run_time_limit      = max_time_model,
                     include_estimators      = use_est,
                     exclude_estimators      = exc_est,
                     include_preprocessors   = use_preproc,
                     exclude_preprocessors   = exc_preproc,
                     n_jobs                  = allowed_jobs,
                     ensemble_size           = allowed_jobs,
                     seed                    = rand_seed,
                     initial_configurations_via_metalearning = 0,
                     resampling_strategy                     = 'cv-iterative-fit',
                     resampling_strategy_arguments           = strategy_args
                     )
 
                     # metric                  = autosklearn.metrics.roc_auc

        attrNames = all_X.columns.values.tolist()
        senseList = self.generateSenseList(attrNames, estVehicle.getAttrSenses())

        print('\nExplore: Attribute names: ' + str(attrNames))
        print('Explore: Selected senses: ' + str(senseList))

        X_train_bal, y_train_bal = self.balanceSamples(X_train, y_train, estVehicle)
        print('\nTraining dataset size: ' + str(X_train_bal.shape))
        print('Test dataset size: ' + str(X_test.shape))

        print("\nTraining the AutoSklearnClassifier on the " + estName + " train dataset.\n")
        automl.fit(X_train_bal, y_train_bal, feat_type=senseList, dataset_name=estName)

        end_wc_seconds = time.time()
        print("\n\tElapsed explore time (sec): ", end_wc_seconds - start_wc_seconds)

        self.computeStats(automl, target, X_test, y_test, estVehicle)        
        
        # print("\n\tResults DataFrame: ")
        # print(automl.cv_results_)
        
        print("\n\tStatistics: ")
        print(automl.sprint_statistics())
        
        # print("\n\tMODELS: ")
        # print(automl.show_models())

        return automl




    #
    # Explore models and hyperparameters for the dataset of the
    # documents/rows in `estName`.
    # 
    #
    def finalizeSKLearnEnsemble(self, automl, target, estVehicle, cleanDF):

        estName = estVehicle.getEstimatorName()
        
        start_wc_seconds = time.time()
        
        all_X, all_y = self.splitDatasetXandY(cleanDF, target)

        print('\nFinal ' + estName + ' training using all data and refit().')
        
        all_X_bal, all_y_bal = self.balanceSamples(all_X, all_y, estVehicle)
        automl.refit(all_X_bal, all_y_bal)

        end_wc_seconds = time.time()
        
        print("\n\tElapsed refit time (sec): ", end_wc_seconds - start_wc_seconds)
        
        # print("\n\tResults DataFrame: ")
        # print(automl.cv_results_)
        
        print("\n\tStatistics: ")
        print(automl.sprint_statistics())
        
        return automl


    #
    # Explore models and hyperparameters for the dataset of the
    # documents/rows in `collName`.
    # 
    #
    def exploreEst(self, estVehicle, cleanedClientDB, target, collName):

        print('\nModel search and ensemble generation for dataset ' + collName + ' ...\n')

        srcColl    = pymongo.collection.Collection( cleanedClientDB, collName )
        srcQuery   = {  target : { "$exists": True }}

        cleanDF    = self.loadCleanDF(srcColl, srcQuery, estVehicle, target)
        
        # destColl   = pymongo.collection.Collection( resultClientDB, collName )
        # destColl.drop()

        # print('Explore dataframe:')
        # print(str(cleanDF))
        
        # Use the recorded categorical attribute transforms to convert the
        # categorical attributes to categorical integers expected by autosklearn.
        # Note that the target attribute will be one of those attributes when
        # True == estVehicle.getIsClassification().
        attrSenses = estVehicle.getAttrSenses()
        for attrName, attrSense in attrSenses.items():
            if attrSense == type_utils.SENSE_CATEGORICAL:
                attrEncoder = estVehicle.getAttrTransform(attrName)
                cleanDF[attrName] = attrEncoder.transform(cleanDF[attrName])

        automl = self.exploreSKLearn(target, estVehicle, cleanDF.copy())
        
        automl = self.finalizeSKLearnEnsemble(automl, target, estVehicle, cleanDF.copy())
        
        estVehicle.setAutoSklearnClassifier(automl)



    #
    #
    #
    def setRandomSeeds(self, estVehicle):
        
        rand_seed = estVehicle.getRandomSeed()
        numpy.random.seed(rand_seed)



    #
    #
    #
    def explore(self):


        mUtils = mongo_utils.MongoUtils()

        # Obtain client connection to the cleaned collections database.
        # This is the source for the exploration stage.
        cleaned_uri = self.aConfig.getCleanedURI()
        cleanedClient = mUtils.getMongoClient(cleaned_uri)
        cleanedClientDB = cleanedClient.get_default_database()

        # Obtain client connection to the result documents collections database.
        # This is the destination for the exploration stage.
        # result_uri = self.aConfig.getResultsURI()
        # resultClient = mUtils.getMongoClient(result_uri)
        # resultClientDB = resultClient.get_default_database()


        print('\n=============================================')
        print('\tMODEL STAGE...')
        print('=============================================\n')

        # Get the list of estimators (predictors) that will be constructed.
        estList = self.aConfig.getEstimatorList()

        # Build the schema and transfer raw docs for each estimator.
        for estimator in estList:
            collName = estimator.get(self.aConfig.SRC_COLLNAME)
            target = estimator.get(self.aConfig.TARGET_NAME)
            estVehicle = self.aConfig.getEstVehicle(collName)
            self.setRandomSeeds(estVehicle)
            self.exploreEst(estVehicle, cleanedClientDB, target, collName)
            estVehicle.doFlushAll()



""" When launched as a script, load the configuration settings and run
    the hypotheses exploration stage.
"""
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('Specify the configuration settings as the first and only parameter.')
        sys.exit()

    csFname = sys.argv[1]
    confSettings = config.AhnungConfig(csFname)

    eStage = ExplorationStage(confSettings)

    print(confSettings.aConfig)

    eStage.explore()
    
    

