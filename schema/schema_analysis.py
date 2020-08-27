#!/usr/bin/env python3

import sys
import json
from datetime import datetime as dt

import pymongo
import bson
import numpy

from sklearn import preprocessing

import config
import vehicle
import mongo_utils

from schema import type_utils


#
# Convert the python dictionary `srcDoc` representing extended JSON at named
# `prefix` in the top-level document to a flat namespace document `flatDoc`.
# 
# See also self.analyzeDoc().
#
def flattenDoc(prefix, srcDoc, flatDoc):

    for key, value in srcDoc.items():

        fkey = key
        if '' != prefix:
            fkey = prefix + type_utils.SA_SEPARATOR + key

        aType, aValue = type_utils.ahnungTypeAndValue(value)
        
        if '_id' == key:
            # Ignoring _id in schema analysis for now.
            pass
        elif not aType in [type_utils.TYPE_UNKNOWN, type_utils.TYPE_DICT]:
            # Record the type and value found at this path.
            flatDoc[fkey] = aValue
        elif isinstance( value, dict ):
            # Call self recursively on new path.
            flattenDoc(fkey, value, flatDoc)
        else:
            print('unhandled: ' + fkey + ' -  type: ' + str(type(value)))




class SchemaStage(object):
    """ The Ahnung schema stage pulls documents containing the estimation
        target (prediction target) from the source and places them in the
        raw docs area.  Additionally, it performs a schema analysis of
        the documents that is useful for later stages.  In particular,
        the cleaning and feature selection steps rely on schema information.
    """

    #
    #
    #
    def __init__(self, aConfig):

        self.aConfig = aConfig
        self.datasets = None
        self.rawDocs = None


    #
    #
    #
    def connect_datasets(self):
        pass


    #
    #
    #
    def incr_attr_type(self, typesDict, nextType):
        
        count = typesDict.get(nextType)
        if None == count:
            count = 1
        else:
            count += 1
        typesDict[nextType] = count


    #
    #
    #
    def incr_attr_value(self, valuesDict, nextValue):
        
        nextStr = str(nextValue)
        count = valuesDict.get(nextStr)
        if None == count:
            count = 1
        else:
            count += 1
        valuesDict[nextStr] = count


    #
    #
    #
    def count_attrpath(self, schemaTable, attrpath, attrtype, value):

        entry = schemaTable.get(attrpath)

        if None == entry:
            present_count                   = 0
            mode_count                      = 0
            entry                           = {}
            entry[type_utils.ATTR_TYPES]    = {}
            entry[type_utils.ATTR_VALUES]   = {}
            entry[type_utils.ATTR_MODE]     = value
            entry[type_utils.ATTR_INTSTR]   = 0
            entry[type_utils.ATTR_FLOATSTR] = 0
            schemaTable[attrpath]           = entry
        else:
            present_count = entry[type_utils.PRESENT_COUNT]
            mode_value = entry[type_utils.ATTR_MODE]
            mode_count = entry[type_utils.ATTR_VALUES].get(str(mode_value))

        present_count += 1
        entry[type_utils.PRESENT_COUNT] = present_count
        
        self.incr_attr_type(entry[type_utils.ATTR_TYPES], attrtype)
        self.incr_attr_value(entry[type_utils.ATTR_VALUES], value)
        
        this_count = entry[type_utils.ATTR_VALUES].get(str(value))
        if this_count > mode_count:
            entry[type_utils.ATTR_MODE] = value

        if type_utils.TYPE_STRING == attrtype:
            resultNum = None
            
            try:
                resultNum = int(value)
                isCount = entry[type_utils.ATTR_INTSTR]
                entry[type_utils.ATTR_INTSTR] = 1 + isCount
            except (ValueError, TypeError) as eX:
                pass
            
            if None == resultNum:
                try:
                    resultNum = float(value)
                    isCount = entry[type_utils.ATTR_FLOATSTR]
                    entry[type_utils.ATTR_FLOATSTR] = 1 + isCount
                except (ValueError, TypeError) as eX:
                    pass



    #
    #
    #
    def convert_flat_int(self, flatDoc, fkey, value):
        try:
            flatDoc[fkey] = int(value)
        except (ValueError, TypeError) as eX:
            # Fail to record attributes with conversion errors.
            print('Not an int: ' + str(value))
            pass


    #
    #
    #
    # def convert_flat_long(self, flatDoc, fkey, value):
    #     try:
    #         flatDoc[fkey] = long(value)
    #     except (ValueError, TypeError) as eX:
    #         # Fail to record attributes with conversion errors.
    #         print('Not a long: ' + str(value))
    #         pass


    #
    #
    #
    def convert_flat_float(self, flatDoc, fkey, value):
        try:
            flatDoc[fkey] = float(value)
        except (ValueError, TypeError) as eX:
            # Fail to record attributes with conversion errors.
            print('Not a float: ' + str(value))
            pass


    #
    #
    #
    def convert_flat_date(self, schemaTable, flatDoc, fkey, value):
        try:
            if isinstance( value, str ):
                flatDoc[fkey] = dt.fromisoformat(value)
            elif isinstance( value, dict ):
                vitems = value.items()
                eType  = vitems[0][0]
                eValue = vitems[0][1]
                if '$numberLong' == eType:
                    flatDoc[fkey] = dt.fromtimestamp(eValue)
        except (ValueError, TypeError) as eX:
            # Fail to record attributes with conversion errors.
            print('Not a date: ' + str(value))
            pass


    #
    # Convert the python dictionary `srcDoc` representing extended JSON named
    # `prefix` in the top-level document to a flat namespace document `flatDoc` and
    # update the global schema data in `schemaTable`.
    #
    # See also flattenDoc().
    #
    def analyzeDoc(self, prefix, srcDoc, schemaTable, flatDoc):

        for key, value in srcDoc.items():

            fkey = key
            if '' != prefix:
                fkey = prefix + type_utils.SA_SEPARATOR + key

            aType, aValue = type_utils.ahnungTypeAndValue(value)
            
            if '_id' == key:
                # Ignoring _id in schema analysis for now.
                pass
            elif not aType in [type_utils.TYPE_UNKNOWN, type_utils.TYPE_DICT]:
                # Record the type and value found at this path.
                self.count_attrpath(schemaTable, fkey, aType, value)
                flatDoc[fkey] = aValue
            elif isinstance( value, dict ):
                # Call self recursively on new path.
                self.analyzeDoc(fkey, value, schemaTable, flatDoc)
            else:
                print('unknown: ' + fkey + ' -  type: ' + str(type(value)))
                



    #
    # Using global schema information in `schemaTable`, determine which attributes
    # have a consistent type that can be
    # selected as the main/expected type for the attribute when converting
    # documents to a numpy or pandas row.  Store this information in the
    # metadata database.
    # 
    # The result of this summarization should allow incoming document
    # attributes to be consistently converted to numpy or pandas values
    # (or very occasionally rejected if the conversion is not possible)
    # that would be useful to a machine learning algorithm, such as the
    # sklearn packages.
    #
    # For a regression, the target type must always be converted to float.
    #
    # For a classification, the target is transformed and the transform is
    # recorded for later use reporting the results of predictions.
    #
    def validateSchemaTypes_Aug03(self, schemaTable, docCount, target, estVehicle):

        numTypeList      = [ type_utils.TYPE_FLOAT, type_utils.TYPE_INT, type_utils.TYPE_LONG ]
        minPresent       = self.aConfig.getSchemaAttrMinPresent()
        minTypeAlignment = self.aConfig.getSchemaAttrMinTypeAlignment()
        maxCatVals       = self.aConfig.getSchemaMaxCategoricalValues()
        
        valTypes         = {}
        valSenses        = {}
        rejectAttrs      = {}
        pathModes        = {}
        isRegression     = estVehicle.getIsRegression()
        isClassification = estVehicle.getIsClassification()
            
        for attrPath, mData in schemaTable.items():
            
            presentCount   = mData[type_utils.PRESENT_COUNT]

            valueCount     = len(mData[type_utils.ATTR_VALUES])
            mData[type_utils.UNIQUE_COUNT] = valueCount
            
            attrSufficient = ( presentCount / docCount ) > minPresent
            
            prefType       = None
            
            # Search for a preferred type with better than minTypeAlignment.
            attrTypes      = mData.get(type_utils.ATTR_TYPES)
            for fType, ftCount in attrTypes.items():
                if type_utils.TYPE_STRING == fType:
                    ftCount -= mData[type_utils.ATTR_INTSTR]
                    ftCount -= mData[type_utils.ATTR_FLOATSTR]
                elif type_utils.TYPE_INT == fType:
                    ftCount += mData[type_utils.ATTR_INTSTR]
                elif type_utils.TYPE_FLOAT == fType:
                    ftCount += mData[type_utils.ATTR_INTSTR]
                    ftCount += mData[type_utils.ATTR_FLOATSTR]

                if ( ftCount / docCount ) > minTypeAlignment:
                    prefType = fType
            
            # The target attribute is always retained/mapped.
            # Classification tasks require categorical target.
            # Regression tasks require a numeric/float target.
            if attrPath == target:

                pathModes[attrPath] = mData.get(type_utils.ATTR_MODE)
                
                if isRegression:
                    # Convert the target attribute to a float for regession estimators.
                    valTypes[attrPath]  = type_utils.TYPE_FLOAT
                    valSenses[attrPath] = type_utils.SENSE_NUMERICAL
                elif None != prefType:
                    valTypes[attrPath]  = prefType
                    valSenses[attrPath] = type_utils.SENSE_CATEGORICAL
                else:
                    valTypes[attrPath]  = type_utils.TYPE_INT
                    valSenses[attrPath] = type_utils.SENSE_CATEGORICAL
                    
                if isClassification:
                    # Classification models require a small number of integer results.
                    # Add a transform to map the string representation of all existing
                    # values to a static integer values and back.
                    valTypes[attrPath]  = type_utils.TYPE_STRING
                    valSenses[attrPath] = type_utils.SENSE_CATEGORICAL
                    targetEncoder       = preprocessing.LabelEncoder()
                    aValDict            = mData.get(type_utils.ATTR_VALUES)
                    aValueList          = aValDict.keys()
                    aValueList          = [str(nVal) for nVal in aValueList]
                    targetEncoder.fit(aValueList)
                    estVehicle.setAttrTransform(target, targetEncoder)

            # If the natural/preferred type is numeric, retain the type and
            # set the sense to numerical.
            elif attrSufficient and prefType in numTypeList:
                valTypes[attrPath]  = prefType
                valSenses[attrPath] = type_utils.SENSE_NUMERICAL
                pathModes[attrPath] = mData.get(type_utils.ATTR_MODE)
                
            # Is the number of values present less that the configured maximum
            # supported number of categories?  If so, classify non-numeric as
            # categorical.
            elif attrSufficient and valueCount <= maxCatVals:
                valTypes[attrPath]  = type_utils.TYPE_STRING
                valSenses[attrPath] = type_utils.SENSE_CATEGORICAL
                pathModes[attrPath] = mData.get(type_utils.ATTR_MODE)
                attrEncoder         = preprocessing.LabelEncoder()
                aValDict            = mData.get(type_utils.ATTR_VALUES)
                aValueList          = aValDict.keys()
                aValueList          = [str(nVal) for nVal in aValueList]
                attrEncoder.fit(aValueList)
                estVehicle.setAttrTransform(attrPath, attrEncoder)
                
            # Give up on this feature/attribute.
            else:
                rejAttrStats = {}
                rejAttrStats[type_utils.PRESENT_COUNT] = presentCount
                rejAttrStats[type_utils.UNIQUE_COUNT] = valueCount
                for tCheck in type_utils.LEARNING_TYPES:
                    rejAttrStats[tCheck] = 0 if None == attrTypes.get(tCheck) else attrTypes.get(tCheck) 
                rejectAttrs[attrPath] = rejAttrStats
                
        return valTypes, valSenses, pathModes, rejectAttrs


    #
    # Using global schema information in `schemaTable`, determine which attributes
    # have a consistent type that can be
    # selected as the main/expected type for the attribute when converting
    # documents to a numpy or pandas row.  Store this information in the
    # metadata database.
    # 
    # The result of this summarization should allow incoming document
    # attributes to be consistently converted to numpy or pandas values
    # (or very occasionally rejected if the conversion is not possible)
    # that would be useful to a machine learning algorithm, such as the
    # sklearn packages.
    #
    # For a regression, the target type must always be converted to float.
    #
    # For a classification, the target is transformed and the transform is
    # recorded for later use reporting the results of predictions.
    #
    def validateSchemaTypes(self, schemaTable, docCount, target, estVehicle):

        numTypeList      = [ type_utils.TYPE_FLOAT, type_utils.TYPE_INT, type_utils.TYPE_LONG ]
        minPresent       = self.aConfig.getSchemaAttrMinPresent()
        minTypeAlignment = self.aConfig.getSchemaAttrMinTypeAlignment()
        maxCatVals       = self.aConfig.getSchemaMaxCategoricalValues()
        
        valTypes         = {}
        valSenses        = {}
        rejectAttrs      = {}
        pathModes        = {}
        isRegression     = estVehicle.getIsRegression()
        isClassification = estVehicle.getIsClassification()
            
        for attrPath, mData in schemaTable.items():
            
            rReason        = ''
            presentCount   = mData[type_utils.PRESENT_COUNT]

            valueCount     = len(mData[type_utils.ATTR_VALUES])
            mData[type_utils.UNIQUE_COUNT] = valueCount

            # Verify the attribute/feature has more than one value and
            # is present in enough instances/documents.            
            attrSufficient = False
            if valueCount > 1:
                attrSufficient = ( presentCount / docCount ) > minPresent
                if not attrSufficient:
                    rReason        = 'Feature is missing in too many instances.'
            else:
                rReason        = 'Feature has only one value.'

            
            prefType       = None
            
            # Extend int and float counts for convertible strings.
            attrTypes      = mData.get(type_utils.ATTR_TYPES)
            iCnt           = 0
            fCnt           = 0
            prevCnt        = attrTypes.get(type_utils.TYPE_INT)
            if None != prevCnt:
                iCnt += prevCnt
            prevCnt        = attrTypes.get(type_utils.TYPE_FLOAT)
            if None != prevCnt:
                fCnt += prevCnt
            prevCnt        = mData.get(type_utils.ATTR_INTSTR)
            if None != prevCnt:
                iCnt += prevCnt
                fCnt += prevCnt
            prevCnt        = mData.get(type_utils.ATTR_FLOATSTR)
            if None != prevCnt:
                fCnt += prevCnt
            attrTypes[type_utils.TYPE_INT]   = iCnt
            attrTypes[type_utils.TYPE_FLOAT] = fCnt

            # Search for a preferred type with better than minTypeAlignment.
            maxAlign       = 0
            maxType        = None
            for fType, ftCount in attrTypes.items():
                if ftCount > maxAlign:
                    maxAlign = ftCount
                    maxType  = fType

            if ( maxAlign / docCount ) > minTypeAlignment:
                prefType = maxType

            # The target attribute is always retained/mapped.
            # Classification tasks require categorical target.
            # Regression tasks require a numeric/float target.
            if attrPath == target:

                pathModes[attrPath] = mData.get(type_utils.ATTR_MODE)
                
                if isRegression:
                    # Convert the target attribute to a float for regession estimators.
                    valTypes[attrPath]  = type_utils.TYPE_FLOAT
                    valSenses[attrPath] = type_utils.SENSE_NUMERICAL
                elif None != prefType:
                    valTypes[attrPath]  = prefType
                    valSenses[attrPath] = type_utils.SENSE_CATEGORICAL
                else:
                    valTypes[attrPath]  = type_utils.TYPE_INT
                    valSenses[attrPath] = type_utils.SENSE_CATEGORICAL
                    
                if isClassification:
                    # Classification models require a small number of integer results.
                    # Add a transform to map the string representation of all existing
                    # values to a static integer values and back.
                    valTypes[attrPath]  = type_utils.TYPE_STRING
                    valSenses[attrPath] = type_utils.SENSE_CATEGORICAL
                    targetEncoder       = preprocessing.LabelEncoder()
                    aValDict            = mData.get(type_utils.ATTR_VALUES)
                    aValueList          = aValDict.keys()
                    aValueList          = [str(nVal) for nVal in aValueList]
                    targetEncoder.fit(aValueList)
                    estVehicle.setAttrTransform(target, targetEncoder)

            # If the natural/preferred type is numeric, retain the type and
            # set the sense to numerical.
            elif attrSufficient and prefType in numTypeList:
                valTypes[attrPath]  = prefType
                valSenses[attrPath] = type_utils.SENSE_NUMERICAL
                pathModes[attrPath] = mData.get(type_utils.ATTR_MODE)
                
            # Is the number of values present less that the configured maximum
            # supported number of categories?  If so, classify non-numeric as
            # categorical.
            elif attrSufficient and valueCount <= maxCatVals:
                valTypes[attrPath]  = type_utils.TYPE_STRING
                valSenses[attrPath] = type_utils.SENSE_CATEGORICAL
                pathModes[attrPath] = mData.get(type_utils.ATTR_MODE)
                attrEncoder         = preprocessing.LabelEncoder()
                aValDict            = mData.get(type_utils.ATTR_VALUES)
                aValueList          = aValDict.keys()
                aValueList          = [str(nVal) for nVal in aValueList]
                attrEncoder.fit(aValueList)
                estVehicle.setAttrTransform(attrPath, attrEncoder)
                
            # Give up on this feature/attribute.
            else:
                if attrSufficient:
                    rReason         = 'Feature type not enough numeric instances and too many distinct values for categorical encoding.'
                rejAttrStats = {}
                rejAttrStats[type_utils.PRESENT_COUNT] = presentCount
                rejAttrStats[type_utils.UNIQUE_COUNT] = valueCount
                rejAttrStats[type_utils.REJECT_REASON] = rReason
                for tCheck in type_utils.LEARNING_TYPES:
                    rejAttrStats[tCheck] = 0 if None == attrTypes.get(tCheck) else attrTypes.get(tCheck) 
                rejectAttrs[attrPath] = rejAttrStats
                
        return valTypes, valSenses, pathModes, rejectAttrs


    #
    #
    #
    def calcIntDefault(self, path, allVals, docCount):

        return 0


    #
    #
    #
    def calcFloatDefault(self, path, allVals, docCount):

        return 0.0


    #
    #
    #
    def calcStringDefault(self, path, allVals, docCount):

        return ''


    #
    #
    #
    def calcDateDefault(self, path, allVals, docCount):

        return dt.fromtimestamp(0)


    #
    #
    #
    def calcAttrMedianMeanInt(self, attrVals):

        valList = []
        
        for valStr, count in attrVals.items():

            value = None
            
            try:
                value = int(valStr)
            except (ValueError, TypeError) as eX:
                pass
            
            if None != value:
                valList.extend([value] * count)

        valArr     = numpy.array(valList, dtype=numpy.int32)
        resMedian  = numpy.median(valArr)
        resMean    = numpy.mean(valArr)

        return resMedian, resMean



    #
    #
    #
    def calcAttrMedianMeanFloat(self, attrVals):

        valList = []
        
        for valStr, count in attrVals.items():

            value = None
            
            try:
                value = float(valStr)
            except (ValueError, TypeError) as eX:
                pass
            
            if None != value:
                valList.extend([value] * count)

        valArr     = numpy.array(valList, dtype=numpy.double)
        resMedian  = numpy.median(valArr)
        resMean    = numpy.mean(valArr)

        return resMedian, resMean



    #
    #
    #
    def calcPathDefaultNumerical(self, schemaTable, path, valType, attrMode):

        pEntry      = schemaTable.get(path)
        attrMedian  = None
        attrMean    = None

        resDefault  = 0
        if type_utils.TYPE_INT == valType:
            attrMedian, attrMean  = self.calcAttrMedianMeanInt(pEntry[type_utils.ATTR_VALUES])
        elif type_utils.TYPE_LONG == valType:
            attrMedian, attrMean  = self.calcAttrMedianMeanInt(pEntry[type_utils.ATTR_VALUES])
        elif type_utils.TYPE_FLOAT == valType:
            resDefault            = 0.0
            attrMedian, attrMean  = self.calcAttrMedianMeanFloat(pEntry[type_utils.ATTR_VALUES])

        if None != attrMedian:
            resDefault = attrMedian
        elif None != attrMean:
            resDefault = attrMean
        elif None != attrMode:
            resDefault = attrMode
            
        return resDefault


    #
    #
    #
    def calcPathDefaultCategorical(self, schemaTable, path, attrMode, attrXForm):
        
        resDefault = 0
        if None != attrMode:
            resDefault = attrMode
        else:
            if None != attrXForm:
                # If a transform is present for a categorical value, use the value
                # associated with the first category.
                resDefault = attrXForm.inverse_transform(numpy.array([0]))[0]
            else:
                pEntry                = schemaTable.get(path)
                attrMedian, attrMean  = self.calcAttrMedianMeanInt(pEntry[type_utils.ATTR_VALUES])
                if None != attrMedian:
                    resDefault = int(attrMedian)
        
        return resDefault



    #
    # Using global schema information in `schemaTable`, determine appropriate default
    # values for each path in the flat namespace.  Store this information in the
    # metadata database.
    #
    def calcDefaultVals(self, schemaTable, valTypes, estVehicle, pathModes, docCount):
        
        # minPresent = self.aConfig.getSchemaAttrMinPresent()
        attrSenses = estVehicle.getAttrSenses()
        defValues = {}
            
        for path, valType in valTypes.items():

            defVal     = None
            attrXForm  = None
            
            attrSense  = attrSenses.get(path)
            attrMode   = pathModes.get(path)
            
            valDict = schemaTable.get(path)
            
            
            if type_utils.SENSE_NUMERICAL == attrSense:
                defVal = self.calcPathDefaultNumerical(schemaTable, path, valType, attrMode)
            elif type_utils.SENSE_CATEGORICAL == attrSense:
                attrXForm = estVehicle.getAttrTransform(path)
                defVal = self.calcPathDefaultCategorical(schemaTable, path, attrMode, attrXForm)
            elif None != attrMode:
                # If the mode (most common value) is known, use that as the default.
                defVal = attrMode
            elif type_utils.TYPE_INT == valType:
                allVals = schemaTable.get(path)
                defVal = self.calcIntDefault(path, allVals, docCount)
            elif type_utils.TYPE_LONG == valType:
                allVals = schemaTable.get(path)
                defVal = self.calcIntDefault(path, allVals, docCount)
            elif type_utils.TYPE_FLOAT == valType:
                allVals = schemaTable.get(path)
                defVal = self.calcFloatDefault(path, allVals, docCount)
            elif type_utils.TYPE_STRING == valType:
                allVals = schemaTable.get(path)
                defVal = self.calcStringDefault(path, allVals, docCount)
            elif type_utils.TYPE_DATE == valType:
                allVals = schemaTable.get(path)
                defVal = self.calcDateDefault(path, allVals, docCount)
                
            if None != defVal:
                defValues[path] = defVal

        return defValues


    #
    #
    #
    def saveStats(self, schemaTable, estVehicle):

        statsKeyList  = [ type_utils.PRESENT_COUNT, type_utils.UNIQUE_COUNT, type_utils.ATTR_MODE, type_utils.ATTR_INTSTR, type_utils.ATTR_FLOATSTR ]
        allStats      = estVehicle.getAttrStats()

        for attrPath, mData in schemaTable.items():
            
            pathStats = {}
            
            for stat in statsKeyList:
                pathStats[stat] = mData[stat]
                
            allStats[attrPath] = pathStats
            
        estVehicle.setAttrStats(allStats)


    #
    # Analyze the schema of the documents in `collName`.
    # 
    # The analysis converts embedded document fields into a flat namespace and
    # stores the resulting documents into `rawClientDB`.  Metadata are computed
    # and stored into `metaClientDB`.
    #
    def analyzeEst(self, dsClientDB, rawClientDB, estVehicle, target, collName):

        print('\nSchema analysis for dataset ' + collName + ' ...\n')

        schemaTable = {}
        docCount    = 0

        destColl = pymongo.collection.Collection( rawClientDB, collName )
        destColl.drop()

        srcColl  = pymongo.collection.Collection( dsClientDB, collName )
        srcQuery = {  target : { "$exists": True }}

        for nRaw in srcColl.find( srcQuery ):

            tValue = nRaw.get(target)
            if tValue is None or tValue == '':
                continue

            flatDoc = {}
            self.analyzeDoc('', nRaw, schemaTable, flatDoc)
            docCount += 1
            destColl.insert(flatDoc)
            
        valTypes, valSenses, pathModes, rejAttrs  = self.validateSchemaTypes(schemaTable, docCount, target, estVehicle)
        estVehicle.setAttrDatatypes(valTypes, doFlush=True)
        estVehicle.setAttrSenses(valSenses, doFlush=True)
        estVehicle.setRejectedAttrs(rejAttrs, doFlush=True)
        # print('Path mode analysis: \n' + str(pathModes) )

        defValues = self.calcDefaultVals(schemaTable, valTypes, estVehicle, pathModes, docCount)
        estVehicle.setAttrDefaults(defValues, doFlush=True)

        self.saveStats(schemaTable, estVehicle)

        # print('For ' + collName + ' with target ' + target + ' analyzed ' + str(docCount) + ' documents having ' + str(len(schemaTable)) + ' unique attributes.')
        # print('Analysis: \n' + str(schemaTable) )
        # print('Valid types: \n' + str(valTypes) )
        # print('Default values: \n' + str(defValues) )
        


    #
    #
    #
    def analyze(self):

        mUtils = mongo_utils.MongoUtils()

        # Obtain client connection to the source collections database.
        # This is the source for the schema stage.
        src_uri = self.aConfig.getSourceURI()
        dsClient = mUtils.getMongoClient(src_uri)
        dsClientDB = dsClient.get_default_database()

        # Obtain client connection to the raw documents collections database.
        # This is the destination for the schema stage.
        raw_uri = self.aConfig.getRawDocsURI()
        rawClient = mUtils.getMongoClient(raw_uri)
        rawClientDB = rawClient.get_default_database()

        # Obtain client connection to the metadata collections database.
        # This is the storage location for metadata detected by the schema stage.
        # meta_uri = self.aConfig.getMetaDataURI()
        # metaClient = mUtils.getMongoClient(meta_uri)
        # metaClientDB = metaClient.get_default_database()

        print('\n=============================================')
        print('\tSCHEMA STAGE...')
        print('=============================================\n')

        # Get the list of estimators (predictors) that will be constructed.
        estList = self.aConfig.getEstimatorList()

        # Build the schema and transfer raw docs for each estimator.
        for estimator in estList:
            collName = estimator.get(self.aConfig.SRC_COLLNAME)
            target = estimator.get(self.aConfig.TARGET_NAME)
            vehicle = self.aConfig.getEstVehicle(collName)
            self.analyzeEst(dsClientDB, rawClientDB, vehicle, target, collName)
            vehicle.doFlushAll()


""" When launched as a script, load the configuration settings and run
    the schema analysis stage.
"""
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('Specify the configuration settings as the first and only parameter.')
        sys.exit()

    csFname = sys.argv[1]
    confSettings = config.AhnungConfig(csFname)

    sStage = SchemaStage(confSettings)

    print(confSettings.aConfig)

    sStage.analyze()
    
    

