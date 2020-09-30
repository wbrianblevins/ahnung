#!/usr/bin/env python3

import sys
# import json
import io

import pandas as pd
import numpy

import flask
import flask.views

import config
# import vehicle
# import mongo_utils

import sklearn.model_selection
import sklearn.datasets
import sklearn.metrics

import matplotlib.figure
import matplotlib.pyplot

import PipelineProfiler
from PipelineProfiler import _plot_pipeline_matrix

from schema import type_utils
from schema import schema_analysis

from cleanup import dataset_cleanup





#
# Standard prefixes used to access various charts.
#
CHART_CONFUSION_MATRIX    = 'confusion'
CHART_ROC                 = 'roc'
CHART_ATTRVCAT            = 'attrvscategorical'
CHART_PERM_IMPORTANCE     = 'permimportance'




#
#
#
def getPredPath(vehicle):
    estName = vehicle.getEstimatorName()
    return '/predict/' + estName


#
#
#
def getPredURL(vehicle):
    predPath = getPredPath(vehicle)
    aConfig = vehicle.getAhnungConfig()
    appHostname = aConfig.getServiceHostname()
    appPort = aConfig.getServicePort()
    return 'http://' + appHostname + ':' + str(appPort) + predPath


#
#
#
def getMetaPath(vehicle):
    estName = vehicle.getEstimatorName()
    return '/metadata/' + estName


#
#
#
def getMetaURL(vehicle):
    metaPath = getMetaPath(vehicle)
    aConfig = vehicle.getAhnungConfig()
    appHostname = aConfig.getServiceHostname()
    appPort = aConfig.getServicePort()
    return 'http://' + appHostname + ':' + str(appPort) + metaPath


#
#
#
def getPProfPath(vehicle):
    estName = vehicle.getEstimatorName()
    return '/pipelineprofiler/' + estName


#
#
#
def getPProfURL(vehicle):
    pProfPath = getPProfPath(vehicle)
    aConfig = vehicle.getAhnungConfig()
    appHostname = aConfig.getServiceHostname()
    appPort = aConfig.getServicePort()
    return 'http://' + appHostname + ':' + str(appPort) + pProfPath


#
#
#
def getChartPath(vehicle, prefix):
    estName = vehicle.getEstimatorName()
    chartPath = '/' + prefix + '/' + estName
    return chartPath


#
#
#
def getChartURL(vehicle, prefix):
    chartPath = getChartPath(vehicle, prefix)
    aConfig = vehicle.getAhnungConfig()
    appHostname = aConfig.getServiceHostname()
    appPort = aConfig.getServicePort()
    return 'http://' + appHostname + ':' + str(appPort) + chartPath



#
#
#
def svgOneChartHTMLPage(titleStr, svgData):
    imgTag = flask.Markup(svgData)
    templateResult = flask.render_template('onechart.html', title=titleStr, imgtag=imgTag)
    return templateResult



#
#
#
def svgPluralChartsHTMLPage(titleStr, svgList):
    for idx in range(len(svgList)):
        svgList[idx] = flask.Markup(svgList[idx])
    templateResult = flask.render_template('pluralcharts.html', title=titleStr, taglist=svgList)
    return templateResult



#
#
#
class PredictEndpoint(flask.views.MethodView):
    """ Endpoint handler for the predict endpoint.
    """

    #
    #
    #
    def __init__(self, vehicle):

        self.vehicle = vehicle


    ## def get(self):
    ##     return '<HTLML>HTTP method unimplemented.</HTML>'


    def post(self):
        jsonReq = flask.request.get_json(silent=True)
        # print('Requested JSON type: ' + str(type(jsonReq)))
        # print('Requested JSON string: ' + str(jsonReq))
        estVehicle = self.vehicle
        
        # Flatten the input document to match Ahnung attribute names.
        flatDoc = {}
        schema_analysis.flattenDoc('', jsonReq, flatDoc)
        # print('Flattened JSON type: ' + str(type(flatDoc)))
        # print('Flattened JSON string: ' + str(flatDoc))
        target           = estVehicle.getEstimatorTarget()
        valTypes         = estVehicle.getAttrDatatypes()
        defValues        = estVehicle.getAttrDefaults()
        # print('Value types type: ' + str(type(valTypes)))
        # print('Value types string: ' + str(valTypes))
        
        # Normalize the document to only contain attibute values.
        pathList         = list(valTypes.copy().keys())
        pathList.remove(target)
        normDoc, valList = dataset_cleanup.normalizeToList(flatDoc, pathList, valTypes, defValues, None)
        reqFrame = pd.DataFrame([valList], columns=pathList)
        
        # Transform categorical values.
        attrSenses = estVehicle.getAttrSenses().copy()
        attrSenses.pop(target)
        for attrName, attrSense in attrSenses.items():
            if attrSense == type_utils.SENSE_CATEGORICAL:
                attrEncoder = estVehicle.getAttrTransform(attrName)
                reqFrame[attrName] = attrEncoder.transform(reqFrame[attrName])

        # print('Request frame type: ' + str(type(reqFrame)))
        # print('Request frame string: ' + str(reqFrame))

        # Use the pre-computed model to predict the target for these attributes.
        automl = estVehicle.getAutoSKLearnModel()
        reqArray = reqFrame.to_numpy(dtype='float')
        predicted = automl.predict(reqArray)
        
        # If needed, transform the predicted target value back to its input space.
        targetTransform = estVehicle.getAttrTransform(target)
        if None != targetTransform:
            predReturn = targetTransform.inverse_transform(predicted)[0]
        else:
            predReturn = predicted[0]

        resDict = normDoc.copy()
        target    = self.vehicle.getEstimatorTarget()
        # jsonResultStr = '{ "' + target + '": ' + str(predReturn) + ' }'
        resDict[target] = predReturn

        return flask.Markup(str(resDict))


    ## def put(self):
    ##     return '<HTLML>HTTP method unimplemented.</HTML>'


    ## def patch(self):
    ##     return '<HTLML>HTTP method unimplemented.</HTML>'


    ## def delete(self):
    ##     return '<HTLML>HTTP method unimplemented.</HTML>'




#
#
#
class MetadataEndpoint(flask.views.MethodView):
    """ Endpoint handler for the predict endpoint.
    """

    #
    #
    #
    def __init__(self, vehicle):

        self.vehicle = vehicle


    #
    #
    #
    def getSamplePredictCurl(self):
        predUrl   = getPredURL(self.vehicle)
        defDict   = self.vehicle.getAttrDefaults().copy()
        typeDict  = self.vehicle.getAttrDatatypes().copy()
        
        target    = self.vehicle.getEstimatorTarget()
        
        defDict.pop(target)

        curlStr   = 'curl -d \'{'
        
        attrCount = len(defDict)
        attrIdx   = 0
        havePrev  = False
        
        for path, defValue in defDict.items():
            nType = typeDict[path]
            if nType in [type_utils.TYPE_STRING, type_utils.TYPE_DATE]:
                if havePrev:
                    curlStr += ', \\\n\t'
                curlStr += '"' + path + '": "' + str(defValue) + '"'
                havePrev = True
            elif nType in [type_utils.TYPE_INT, type_utils.TYPE_FLOAT, type_utils.TYPE_LONG]:
                if havePrev:
                    curlStr += ', \\\n\t'
                curlStr += '"' + path + '": ' + str(defValue)
                havePrev = True

            attrIdx += 1

        curlStr += ' }\' -H "Content-Type: application/json" -X POST '
        curlStr += predUrl
        return curlStr


    #
    #
    #
    def genResourceTableRows(self):
        result    = ''
        result   += '<thead>'
        result   += '<th>Name</th>'
        result   += '<th>URL</th>'
        result   += '<th>Description</th>'
        result   += '</thead>'
        result   += '<tbody>'
        result   += '<tr>'
        result   += '<td>Prediction</td>'
        predUrl   = getPredURL(self.vehicle)
        result   += '<td><a href="' + predUrl + '">' + predUrl + '</a></td>'
        curlCmd   = self.getSamplePredictCurl()
        result   += '<td><pre>' + curlCmd + '</pre></td>'
        result   += '</tr>'
        result   += '<tr>'
        result   += '<td>PipelineProfiler</td>'
        pProfUrl  = getPProfURL(self.vehicle)
        result   += '<td><a href="' + pProfUrl + '">' + pProfUrl + '</a></td>'
        result   += '<td><a href="https://towardsdatascience.com/exploring-auto-sklearn-models-with-pipelineprofiler-5b2c54136044">PipelineProfiler Blog Post</a></td>'
        result   += '</tr>'
        if self.vehicle.getIsClassification():
            result       += '<tr>'
            result       += '<td>Confusion Matrix: True vs Predicted</td>'
            confusionUrl  = getChartURL(self.vehicle, CHART_CONFUSION_MATRIX)
            result       += '<td><a href="' + confusionUrl + '">' + confusionUrl + '</a></td>'
            result       += '<td><a href="https://scikit-learn.org/stable/modules/model_evaluation.html#confusion-matrix">Confusion Matrix Description</a></td>'
            result       += '</tr>'
            result       += '<tr>'
            result       += '<td>ROC Curve Charts</td>'
            rocUrl        = getChartURL(self.vehicle, CHART_ROC)
            result       += '<td><a href="' + rocUrl + '">' + rocUrl + '</a></td>'
            result       += '<td><a href="https://scikit-learn.org/stable/modules/generated/sklearn.metrics.roc_curve.html#sklearn.metrics.roc_curve">ROC Curve Description</a></td>'
            result       += '</tr>'
        result       += '<tr>'
        result       += '<td>Feature Importance via Permutation</td>'
        permiUrl      = getChartURL(self.vehicle, CHART_PERM_IMPORTANCE)
        result       += '<td><a href="' + permiUrl + '">' + permiUrl + '</a></td>'
        result       += '<td><a href="https://scikit-learn.org/stable/modules/generated/sklearn.inspection.permutation_importance.html#sklearn.inspection.permutation_importance">Permutation Importance Description</a></td>'
        result       += '</tr>'
        result   += '</tbody>'
        return flask.Markup(result)


    #
    #
    #
    def genSelectedAttrTableRows(self):
        typeDict   = self.vehicle.getAttrDatatypes().copy()
        senseDict  = self.vehicle.getAttrSenses().copy()
        
        target     = self.vehicle.getEstimatorTarget()
        
        typeDict.pop(target)
        senseDict.pop(target)

        result    = ''
        result   += '<thead>'
        result   += '<th>Name</th>'
        result   += '<th>Type</th>'
        result   += '<th>Sense</th>'
        result   += '</thead>'
        result   += '<tbody>'
        
        for nPath, nType in typeDict.items():
            result   += '<tr>'
            result   += '<td>' + nPath + '</td>'
            result   += '<td>' + nType + '</td>'
            result   += '<td>' + senseDict[nPath] + '</td>'
            result   += '</tr>'

        result   += '</tbody>'
        return flask.Markup(result)


    #
    #
    #
    def genRejectedAttrTableRows(self):
        rejDict   = self.vehicle.getRejectedAttrs().copy()

        result    = ''
        result   += '<thead>'
        result   += '<th>Name</th>'
        result   += '<th>Present</th>'
        result   += '<th>Distinct</th>'
        result   += '<th>Type(s)</th>'
        result   += '<th>Reject Reason</th>'
        result   += '</thead>'
        result   += '<tbody>'

        if len(rejDict) > 0:
            for nPath, nData in rejDict.items():
    
                fTypeCounts = {}
                for tCheck in type_utils.LEARNING_TYPES:
                    if None != nData.get(tCheck):
                        if 0 < nData[tCheck]:
                            fTypeCounts[tCheck] = nData[tCheck]
                
                if 0 == len(fTypeCounts):
                    typeCntStr = 'None detected'
                else:
                    typeCntStr = str(fTypeCounts)
    
                result   += '<tr>'
                result   += '<td>' + nPath + '</td>'
                result   += '<td>' + str(nData[type_utils.PRESENT_COUNT]) + '</td>'
                result   += '<td>' + str(nData[type_utils.UNIQUE_COUNT]) + '</td>'
                result   += '<td>' + typeCntStr + '</td>'
                result   += '<td>' + str(nData[type_utils.REJECT_REASON]) + '</td>'
                result   += '</tr>'
        else:
            result   += '<tr>'
            result   += '<td>N/A</td>'
            result   += '<td>N/A</td>'
            result   += '<td>N/A</td>'
            result   += '<td>N/A</td>'
            result   += '<td>N/A</td>'
            result   += '</tr>'

        result   += '</tbody>'
        return flask.Markup(result)



    #
    #
    #
    def genPerformanceAttrChart(self):
        allStats       = self.vehicle.getAttrStats().copy()
        recallDict     = allStats[type_utils.STATS_RECALL_SCORE]
        precisionDict  = allStats[type_utils.STATS_PRECISION_SCORE]
        rocaucDict     = allStats[type_utils.STATS_ROCAUC_SCORE]

        estVehicle = self.vehicle
        estName = estVehicle.getEstimatorName()

        labels = ['Precision', 'Recall', 'ROC AUC']

        fig = matplotlib.figure.Figure()
        fax = fig.subplots()
        fax.set_title("Ensemble Performance on " + estName)

        # cmd.plot(ax=fax)
        targets  = precisionDict.keys()
        tCnt     = len(targets)
        vals     = []
        rects    = []
        xtPos    = numpy.arange(len(labels))  # the label locations
        bWidth   = 0.8
        gWidth   = bWidth / tCnt
        
        idx = 0
        for tName in targets:
            newVals = []
            newVals.append(precisionDict.get(tName))
            newVals.append(recallDict.get(tName))
            newVals.append(rocaucDict.get(tName))
            vals.append(newVals)
            newRects = fax.bar(xtPos + gWidth*idx, newVals, gWidth, label=tName)
            rects.append(newRects)
            for rect in newRects:
                height = rect.get_height()
                fax.annotate('{:03.3f}'.format(height),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')
            idx += 1

        # Add some text for labels, title and custom x-axis tick labels, etc.
        fax.set_ylabel('Performance Score')
        fax.set_title('Performance by Target Label')
        midOffset = ((bWidth / 2) - (gWidth / 2))
        fax.set_xticks(xtPos + midOffset)
        fax.set_xticklabels(labels)
        fax.legend()

        buf = io.BytesIO()
        fig.savefig(buf, format="svg")
        # Embed the result in the html output.
        data = buf.getvalue().decode("utf-8")
        
        matplotlib.pyplot.close(fig)
            
        return flask.Markup(data)





    def get(self):

        automl          = None
        templateResult  = None
        htmlResult      = '<HTLML>AutoSKLearn model not defined.</HTML>'

        if self.vehicle.getIsClassification():
            automl = self.vehicle.getAutoSklearnClassifier()

        if self.vehicle.getIsRegression():
            automl = self.vehicle.getAutoSklearnRegressor()

        if None != automl:
            titleStr        = self.vehicle.getEstimatorName()
            perfChart       = self.genPerformanceAttrChart()
            estStats        = automl.sprint_statistics()
            resRows         = self.genResourceTableRows()
            selRows         = self.genSelectedAttrTableRows()
            rejRows         = self.genRejectedAttrTableRows()
            templateResult  = flask.render_template('metadata.html', title=titleStr, perfStatistics=perfChart, estStatistics=estStats, resourceRows=resRows, selectedAttrRows=selRows, rejectedAttrRows=rejRows)

        if None != templateResult:
            htmlResult = templateResult

        return htmlResult





#
#
#
class PipelineProfilerEndpoint(flask.views.MethodView):
    """ Endpoint handler for the predict endpoint.
    """

    #
    #
    #
    def __init__(self, vehicle):

        self.vehicle = vehicle


    def get(self):

        automl = None
        htmlResult = '<HTLML>AutoSKLearn model not defined.</HTML>'

        if self.vehicle.getIsClassification():
            automl = self.vehicle.getAutoSklearnClassifier()

        if self.vehicle.getIsRegression():
            automl = self.vehicle.getAutoSklearnRegressor()

        if None != automl:
            # mod_list_s = automl.show_models()
            # print('\n\nModels:\n' + str(mod_list_s))
            # mod_list_w = automl.get_models_with_weights()
            # print('\n\nModels and weights:\n' + str(mod_list_w))

            profiler_pipeline = PipelineProfiler.import_autosklearn(automl)
            # PipelineProfiler.plot_pipeline_matrix(profiler_data)

            id = _plot_pipeline_matrix.id_generator()
            data_dict = _plot_pipeline_matrix.prepare_data_pipeline_matrix(profiler_pipeline)
            htmlResult = _plot_pipeline_matrix.make_html(data_dict, id)

        return htmlResult



#
#
#
class ConfusionEndpoint(flask.views.MethodView):
    """ Endpoint handler for the confusion matrix chart endpoint.
    """

    #
    #
    #
    def __init__(self, vehicle):

        self.vehicle = vehicle


    def get(self):

        automl = None
        estVehicle = self.vehicle
        estName = estVehicle.getEstimatorName()

        htmlResult = '<HTLML>AutoSKLearn model not defined.</HTML>'

        if estVehicle.getIsClassification():
            automl = estVehicle.getAutoSklearnClassifier()

        if estVehicle.getIsRegression():
            htmlResult = '<HTLML>AutoSKLearn model is not a classification.</HTML>'

        if None != automl:
            # profiler_pipeline = PipelineProfiler.import_autosklearn(automl)
            # PipelineProfiler.plot_pipeline_matrix(profiler_data)

            allStats = estVehicle.getAttrStats().copy()
            y_test = allStats[type_utils.STATS_Y_TESTING]
            y_pred = allStats[type_utils.STATS_Y_PREDICT]

            target = estVehicle.getEstimatorTarget()
            targetTransform = estVehicle.getAttrTransform(target)
            
            raw_y_test = targetTransform.transform(y_test)
            raw_y_pred = targetTransform.transform(y_pred)
            
            labels = estVehicle.getTargetLabels()
            cm = sklearn.metrics.confusion_matrix(raw_y_test, raw_y_pred, normalize='true')
            cmd = sklearn.metrics.ConfusionMatrixDisplay(cm, display_labels=labels)
            
            fig = matplotlib.figure.Figure()
            fax = fig.subplots()
            fax.set_title(estName + " Confusion Matrix")

            cmd.plot(ax=fax)
            
            buf = io.BytesIO()
            fig.savefig(buf, format="svg")
            # Embed the result in the html output.
            data = buf.getvalue().decode("utf-8")
            
            matplotlib.pyplot.close(fig)
            
            thisTitle = estVehicle.getEstimatorName() + ' Confusion Matrix'
            htmlResult = svgOneChartHTMLPage(titleStr=thisTitle, svgData=data)

        return htmlResult




#
#
#
class ROCEndpoint(flask.views.MethodView):
    """ Endpoint handler for the ROC charts endpoint.
    """

    #
    #
    #
    def __init__(self, vehicle):

        self.vehicle = vehicle


    def get(self):

        estVehicle = self.vehicle
        estName = estVehicle.getEstimatorName()
        htmlResult = '<HTLML>AutoSKLearn model not defined.</HTML>'

        if estVehicle.getIsRegression():
            htmlResult = '<HTLML>AutoSKLearn model is not a classification.</HTML>'

        if estVehicle.getIsClassification():

            allStats = estVehicle.getAttrStats().copy()
            fprDict = allStats[type_utils.STATS_FPR]
            tprDict = allStats[type_utils.STATS_TPR]

            target = estVehicle.getEstimatorTarget()
            targetEncoder = estVehicle.getAttrTransform(target)
            
            tagList = []
            
            labels = estVehicle.getTargetLabels()
            for targetClass in labels:
                tcIdx = targetEncoder.transform(numpy.array([targetClass]))[0]

                fpr = numpy.array(fprDict[str(tcIdx)])
                tpr = numpy.array(tprDict[str(tcIdx)])
                
                roc_auc = sklearn.metrics.auc(fpr, tpr)
                title = targetClass + ' as positive in ' + estName + ' estimator'
                display = sklearn.metrics.RocCurveDisplay(fpr=fpr, tpr=tpr, roc_auc=roc_auc, estimator_name=title)

                fig = matplotlib.figure.Figure()
                fax = fig.subplots()
    
                display.plot(ax=fax)
                fax.set_title(targetClass + " ROC Curve in " + estName)
                
                buf = io.BytesIO()

                fig.savefig(buf, format="svg")
                # Embed the result in the html output.
                data = buf.getvalue().decode("utf-8")
                
                matplotlib.pyplot.close(fig)

                tagList.append(data)
                        
            thisTitle = estName + ' ROC Curves'
            htmlResult = svgPluralChartsHTMLPage(titleStr=thisTitle, svgList=tagList)

        return htmlResult



#
#
#
class PermImpEndpoint(flask.views.MethodView):
    """ Endpoint handler for the Permutation Importance chart endpoint.
    """

    #
    #
    #
    def __init__(self, vehicle):

        self.vehicle = vehicle


    def get(self):

        estVehicle = self.vehicle
        estName = estVehicle.getEstimatorName()
        htmlResult = '<HTLML>AutoSKLearn model not defined.</HTML>'

        if estVehicle.getIsClassification():
            automl = estVehicle.getAutoSklearnClassifier()

        if estVehicle.getIsRegression():
            automl = estVehicle.getAutoSklearnRegressor()

        if None != automl:
            # profiler_pipeline = PipelineProfiler.import_autosklearn(automl)
            # PipelineProfiler.plot_pipeline_matrix(profiler_data)

            allStats     = estVehicle.getAttrStats().copy()
            col_names    = numpy.array(allStats[type_utils.STATS_ATTR_NAMES])
            imp_means    = numpy.array(allStats[type_utils.STATS_PERMI_MEAN])
            # imp_stds     = numpy.array(allStats[type_utils.STATS_PERMI_STD])
            imp_vals     = numpy.array(allStats[type_utils.STATS_PERMI_VALS])

            sorted_idx   = numpy.flip(imp_means.argsort())

            tagList      = []
            maxPerChart  = 20
            attrIdx      = 0
            
            while attrIdx < len(sorted_idx):

                attrCnt = min(maxPerChart, (len(sorted_idx) - attrIdx))

                fig = matplotlib.figure.Figure()
                fax = fig.subplots()
                
                cSlice = imp_vals[sorted_idx[attrIdx:attrIdx+attrCnt]]
                cVals = numpy.flip(cSlice).T
                cLabels = numpy.flip(col_names[sorted_idx[attrIdx:attrIdx+attrCnt]])
                fax.boxplot(cVals, vert=False, labels=cLabels)
                fax.set_title(estName + " Feature Importance by Permutation")
                fig.tight_layout()            
                            
                buf = io.BytesIO()
                fig.savefig(buf, format="svg")
                # Embed the result in the html output.
                data = buf.getvalue().decode("utf-8")
                
                matplotlib.pyplot.close(fig)
                tagList.append(data)
                attrIdx += attrCnt

            thisTitle = estName + ' Feature Importance'
            htmlResult = svgPluralChartsHTMLPage(titleStr=thisTitle, svgList=tagList)

        return htmlResult



class ServiceStage(object):
    """ The Ahnung prediction/service stage provides a REST API for accessing the generated
        estimator and predicting unseen instances.  Additionally, it provides a web interface
        to summary statistics about the dataset and the estimator (prediction engine).
    """

    #
    #
    #
    def __init__(self, aConfig):

        self.aConfig = aConfig


    #
    #
    #
    def registerVehicleCharts(self, flaskApp, vehicle):
        estName = vehicle.getEstimatorName()
        
        # ConfusionEndpoint
        urlPathConfusion  = getChartPath(vehicle, CHART_CONFUSION_MATRIX)
        # confusionUrl      = getChartURL(vehicle, CHART_CONFUSION_MATRIX)
        appNameConfusion  = CHART_CONFUSION_MATRIX + '_' + estName
        flaskApp.add_url_rule(urlPathConfusion, view_func=ConfusionEndpoint.as_view(appNameConfusion, vehicle=vehicle))

        # ROCEndpoint
        urlPathROC        = getChartPath(vehicle, CHART_ROC)
        # rocUrl            = getChartURL(vehicle, CHART_ROC)
        appNameROC        = CHART_ROC + '_' + estName
        flaskApp.add_url_rule(urlPathROC, view_func=ROCEndpoint.as_view(appNameROC, vehicle=vehicle))

        # urlPathAttrVCat   = getChartPath(vehicle, CHART_ATTRVCAT)
        # # attrVCatUrl       = getChartURL(vehicle, CHART_ATTRVCAT)
        # appNameAttrVCat   = CHART_ATTRVCAT + '_' + estName
        # flaskApp.add_url_rule(urlPathAttrVCat, view_func=AttributeVsCategoricalEndpoint.as_view(appNameAttrVCat, vehicle=vehicle))

        # PermImpEndpoint
        urlPathPermImp   = getChartPath(vehicle, CHART_PERM_IMPORTANCE)
        # rocUrl           = getChartURL(vehicle, CHART_PERM_IMPORTANCE)
        appNamePermImp   = CHART_PERM_IMPORTANCE + '_' + estName
        flaskApp.add_url_rule(urlPathPermImp, view_func=PermImpEndpoint.as_view(appNamePermImp, vehicle=vehicle))



    #
    #
    #
    def registerVehicle(self, flaskApp, vehicle, target, collName):

        vehicle.doLoadModel()

        estName = vehicle.getEstimatorName()

        appHostname = self.aConfig.getServiceHostname()
        appPort = self.aConfig.getServicePort()

        urlPathPred = getPredPath(vehicle)
        appNamePred = 'predict_' + estName
        flaskApp.add_url_rule(urlPathPred, view_func=PredictEndpoint.as_view(appNamePred, vehicle=vehicle))

        # fullUrlPred = getPredURL(vehicle)
        # print('\nPrediction URL for ' + estName + ' is:\n\t' + fullUrlPred)

        urlPathMeta = getMetaPath(vehicle)
        appNameMeta = 'metadata_' + estName
        flaskApp.add_url_rule(urlPathMeta, view_func=MetadataEndpoint.as_view(appNameMeta, vehicle=vehicle))

        fullUrlMeta = getMetaURL(vehicle)
        print('\nMetadata summary URL for ' + estName + ' is:\n\t' + fullUrlMeta + '\n')

        urlPathPProf = getPProfPath(vehicle)
        appNamePProf = 'pipelineprofiler_' + estName
        flaskApp.add_url_rule(urlPathPProf, view_func=PipelineProfilerEndpoint.as_view(appNamePProf, vehicle=vehicle))

        # fullUrlPProf = getPProfURL(vehicle)
        # print('Pipeline profiler URL for ' + estName + ' is:\n\t' + fullUrlPProf + '\n')

        self.registerVehicleCharts(flaskApp, vehicle)



    #
    #
    #
    def runWebService(self, flaskApp):

        appHostname = self.aConfig.getServiceHostname()
        appPort = self.aConfig.getServicePort()

        flaskApp.run(host=appHostname, port=appPort)



    #
    #
    #
    def serve(self):

        flaskApp = flask.Flask('AhnungPredictionEngine')

        print('\n=============================================')
        print('\tPREDICT STAGE...')
        print('=============================================\n')

        # Get the list of estimators (predictors) that will be served.
        estList = self.aConfig.getEstimatorList()

        # Build the schema and transfer raw docs for each estimator.
        for estimator in estList:
            collName = estimator.get(self.aConfig.SRC_COLLNAME)
            target = estimator.get(self.aConfig.TARGET_NAME)
            vehicle = self.aConfig.getEstVehicle(collName)
            self.registerVehicle(flaskApp, vehicle, target, collName)

        self.runWebService(flaskApp)



""" When launched as a script, load the configuration settings and run
    the predict/rest/service analysis stage.
"""
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print('Specify the configuration settings as the first and only parameter.')
        sys.exit()

    csFname = sys.argv[1]
    confSettings = config.AhnungConfig(csFname)

    sStage = ServiceStage(confSettings)

    print(sStage.aConfig)

    sStage.serve()



