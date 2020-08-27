#!/usr/bin/env python3

import sys
import json
from datetime import datetime as dt

import pymongo
import bson

import config
import mongo_utils


#
# Dictionary keys (constants) used in the schema analysis.
#
PRESENT_COUNT      = 'present_count'
ATTR_TYPES         = 'attr_types'
ATTR_VALUES        = 'attr_values'
ATTR_INTSTR        = 'attr_intstr'
ATTR_FLOATSTR      = 'attr_floatstr'
ATTR_MODE          = 'attr_mode'
UNIQUE_COUNT       = 'unique_count'
REJECT_REASON      = 'reject_reason'

#
# Attribute path separator character.
#
SA_SEPARATOR       = '.'

#
# Dictionary keys (constants) used for estimator statistics.
#
STATS_Y_TESTING        = 'y_testing'
STATS_Y_PREDICT        = 'y_predict'
STATS_FPR              = 'false_positive_rate'
STATS_TPR              = 'true_positive_rate'
STATS_ATTR_NAMES       = 'attribute_names'
STATS_PERMI_MEAN       = 'perm_importance_means'
STATS_PERMI_STD        = 'perm_importance_stds'
STATS_PERMI_VALS       = 'perm_importance_values'
STATS_PRECISION_SCORE  = 'precision_score'
STATS_RECALL_SCORE     = 'recall_score'
STATS_ROCAUC_SCORE     = 'rocauc_score'

#
# Names of types tracked in the schema analysis.
#
TYPE_INT           = 'int'
TYPE_LONG          = 'long'
TYPE_FLOAT         = 'float'
TYPE_STRING        = 'string'
TYPE_DATE          = 'date'

LEARNING_TYPES = [TYPE_INT, TYPE_LONG, TYPE_FLOAT, TYPE_STRING, TYPE_DATE]

#
# Names of logical types used internally to Ahnung.
#
TYPE_DICT          = 'dict'
TYPE_UNKNOWN       = 'unknown'

#
# Names of attribute senses tracked in the schema analysis for use during
# ensemble evaluation and training.
#
SENSE_NUMERICAL    = "Numerical"
SENSE_CATEGORICAL  = "Categorical"

#
# Meta data collection name suffixes
#
TYPES_SUFFIX       = '_types'
DEFAULTS_SUFFIX    = '_defaults'
SENSES_SUFFIX      = '_sense'
REJECT_SUFFIX      = '_reject'
FSIDS_SUFFIX       = '_gridfsids'
STATS_SUFFIX       = '_stats'


#
# Default values for missing fields or incorrect types.
#
MISSING_INT     = 0
MISSING_FLOAT   = 0.0
MISSING_STRING  = ''
MISSING_DATE    = dt.fromtimestamp(0)



#
#
#
def convert_int(value, default, counter):
    resultInt = default

    try:
        resultInt = int(value)
    except (ValueError, TypeError) as eX:
        # Fail to record attributes with conversion errors.
        counter += 1
        print('Not an int: ' + str(value))

    return resultInt, counter


#
#
#
def convert_float(value, default, counter):
    resultFloat = default

    try:
        resultFloat = float(value)
    except (ValueError, TypeError) as eX:
        # Fail to record attributes with conversion errors.
        counter += 1
        print('Not a float: ' + str(value))

    return resultFloat, counter


#
#
#
def convert_date(value, default, counter):
    resultDate = default

    try:
        if isinstance( value, str ):
            resultDate = dt.fromisoformat(value)
        elif isinstance( value, dict ):
            vitems = value.items()
            eType  = vitems[0][0]
            eValue = vitems[0][1]
            if '$numberLong' == eType:
                resultDate = dt.fromtimestamp(eValue)
            else:
                counter += 1
    except (ValueError, TypeError) as eX:
        # Fail to record attributes with conversion errors.
        counter += 1
        print('Not a date: ' + str(value))

    return resultDate, counter


#
#
#
def convert_string(value, default, counter):
    resultString = default

    try:
        resultString = str(value)
    except (ValueError, TypeError) as eX:
        # Fail to record attributes with conversion errors.
        counter += 1
        print('Not a string: ' + str(value))

    return resultString, counter



#
#
#
def ahnungType(tValue):

    result = TYPE_UNKNOWN

    if isinstance( tValue, int ):
        result = TYPE_INT
    elif isinstance( tValue, float ):
        result = TYPE_FLOAT
    elif isinstance( tValue, str ):
        result = TYPE_STRING
    elif isinstance( tValue, dict ):
        result = TYPE_DICT
        vitems = tValue.items()
        if len(vitems) == 1:
            if isinstance( vitems[0][0], str ):
                eType  = vitems[0][0]
                eValue = vitems[0][1]
                if '$numberDouble' == eType:
                    result = TYPE_FLOAT
                elif '$numberLong' == eType:
                    result = TYPE_INT
                elif '$numberInt' == eType:
                    result = TYPE_INT
                elif '$date' == eType:
                    result = TYPE_DATE

    return result


#
#
#
def ahnungTypeAndValue(tValue):

    resType  = TYPE_UNKNOWN
    resValue = tValue
    counter  = 0

    if isinstance( tValue, int ):
        resType = TYPE_INT
    elif isinstance( tValue, float ):
        resType = TYPE_FLOAT
    elif isinstance( tValue, str ):
        resType = TYPE_STRING
    elif isinstance( tValue, dict ):
        resType = TYPE_DICT
        vitems = tValue.items()
        if len(vitems) == 1:
            if isinstance( vitems[0][0], str ):
                eType  = vitems[0][0]
                eValue = vitems[0][1]
                if '$numberDouble' == eType:
                    resType  = TYPE_FLOAT
                    resValue, counter = convert_float(eValue, MISSING_FLOAT, counter)
                elif '$numberLong' == eType:
                    resType  = TYPE_INT
                    resValue, counter = convert_int(eValue, MISSING_INT, counter)
                elif '$numberInt' == eType:
                    resType  = TYPE_INT
                    resValue, counter = convert_int(eValue, MISSING_INT, counter)
                elif '$date' == eType:
                    resType  = TYPE_DATE
                    resValue, counter = convert_date(eValue, MISSING_DATE, counter)

    return resType, resValue





