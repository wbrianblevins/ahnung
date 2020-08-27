#!/usr/bin/env python

import sys
import json

import config
from schema import schema_analysis
from cleanup import dataset_cleanup
from model import explore_hypotheses
from predict import serve_rest

STAGE_SCHEMA   = 'schema'
STAGE_CLEANUP  = 'cleanup'
STAGE_MODEL    = 'model'
STAGE_PREDICT  = 'predict' 
stageNames = [STAGE_SCHEMA, STAGE_CLEANUP, STAGE_MODEL, STAGE_PREDICT]


""" When launched as a script, load the configuration settings and run
    the schema analysis stage.
"""
if __name__ == "__main__":

    if len(sys.argv) < 2:
        print('Specify the configuration settings JSON file as the first parameter.')
        print('USAGE: ' + sys.argv[0] + ' <settings.json> [first_stage [second_stage [...]]]')
        print('    - Valid stages are: schema, cleanup, model and predict.')
        sys.exit()

    csFname = sys.argv[1]
    confSettings = config.AhnungConfig(csFname)
    
    stageList = None
    nStage = 2
    while len(sys.argv) > nStage:
        if None == stageList:
            stageList = [sys.argv[nStage]]
        else:
            stageList.append(sys.argv[nStage])
        nStage += 1

    if None == stageList or STAGE_SCHEMA in stageList:
        sStage = schema_analysis.SchemaStage(confSettings)
        sStage.analyze()

    if None == stageList or STAGE_CLEANUP in stageList:
        cStage = dataset_cleanup.CleanupStage(confSettings)
        cStage.cleanup()

    if None == stageList or STAGE_MODEL in stageList:
        eStage = explore_hypotheses.ExplorationStage(confSettings)
        eStage.explore()

    if None == stageList or STAGE_PREDICT in stageList:
        pStage = serve_rest.ServiceStage(confSettings)
        pStage.serve()



