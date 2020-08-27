

# ToDo Items

### Regression

- Add support for regression tasks.  Requires global code review.  Many updates required, particularly in explore_hypotheses.py and serve_rest.py.


### Feature Engineering

- Find or implement feature tranforms (feature engineering) for common datatypes that allow the discovery of common patterns.  Should these transforms be in Ahnung or possibly implemented as preprocessor stages in AutoSKLearn?  What sense should a known type carry into AutoSKLearn when such a preprocessor stage is used?
    - date-time: year
    - date-time: month of year
    - date-time: day of month
    - date-time: day of week
    - date-time: hour of day


- Improved default value selection in schema/schema_analysis.py
- Improve the handling of missing numerical and categorical values.  Allow the Ahnung user to select one of the common approaches via "missingValue" setting: "default", "mean", "median", "mode", "alwaysInstance", "alwaysFeature".
    - Default approach:
        - If too many features missing in the same instance/document, drop/remove that instance/document with missing featues.
        - If less than attr_type_min_present available for a feature, drop/remove the feature/attribute/column with missing values.
        - Otherwise, replace missing value statistically: categorical via mode, numeric via median.
    - Replace missing value statistically: categorical via mode, numeric via mean/median/mode
    - Always drop/remove instances/documents/rows with missing values.
    - Always drop/remove features/attributes/columns with missing values.
    - Optionally generate new boolean feature signaling whether an attribute is missing from the instance (stretch?).  This implies a new layer of feature engineering with Ahnung adding features.  Possibly easiest to add those during the cleanup stage.
    - Impute missing values using ML engine against other attributes (stretch?).



- For non-numeric, potential categorical attributes with more than maxVals = getSchemaMaxCategoricalValues() distinct values present, find/use/implement a categorical transform that encodes the most common maxVals-1 as distinct integer/categorical values and combines all less common values as a single other value/encoding.
    - As with a boolean flag for sparse values, this implies a new layer of feature engineering from Ahnung.


### AutoSKLearn Configuration


- Move the AutoSKLearn settings into estimator/vehicle configuration.  Needed to allow different settings per workload/vehicle/estimator.  With per-vehicle settings, a single run can evaluate multiple AutoSKLearn configurations against the same dataset/task.
    - CPUs, global time, per-model time [done]
    - use/exclude estimators
    - use/exclude preprocessors


- Consider cross-validation with resampling_strategy='cv', resampling_strategy_arguments={'folds': 5} and automl.refit(X_all, y_all).  Make these configurable.

### Visualize Estimator and Results

- Enhance the predict stage to provide an appropriate REST interface to estimator summary statistics, including most accurate ensemble components and random forest analysis summary.
    - estimator summary statistics [done]
    - feature importance [done]
    - random forest analysis summary


- Implement specification of list of attributes forming the `inputs` to the prediction/estimation process.  Filter docs in `schema` stage missing any/some `inputs`.  Filter docs in `cleaning` stage with `inputs` that can not be successfully converted to the standardized/uniform type.


### Cleanup

- Implement outlier suppression.


- Implement a set of standardized value transform utilities.
    - Categorical target string to integer, forward and reverse.
    - [DONE] Extended JSON to flat python dictionary with python types.
    - String JSON to flat python dictionary with python types.
    - Flat python dictionary with python types to panda row.





# Completed Items



- Improve the computation and display of estimator global results on held-out data.  This should include ROC AUC, recall and precision.


- Track rejected attributes in schema_analysis and report in predict stage.

- Track useful data per path/attribute during schema analysis.  This data should improve type selection/validation and/or improve dataset summary report in the predict stage.  The path keyed dictionary will be stored/loaded using vehicle.getAttrStats() and vehicle.setAttrStats().
    - How many instance attributes are strings convertible to int?
    - How many instance attributes are strings convertible to float?
    - How many instance attributes are present.


- Implement predict stage providing an appropriate REST interface to an estimation service.

- Verify that autosklearn_model.refit() is used to apply held-out data before offering the predition service.  Necessary to make sure the ensemble models have seen 100% of the available training data.

- Verify that instances (docs/rows) without a target value are discarded (rather than using the default value).

- Scan attributes in the schema stage to establish preferred senses or "feature types" for classifier.fit(X_train, Y_train, feat_type=feat_type).


- Support recognition of categorical target attributes as strings, provide conversion for
  supervised learning as well as for prediction.
    - preprocessing.LabelEncoder().fit_transform(y)
    - https://automl.github.io/auto-sklearn/master/examples/example_feature_types.html

- Detect and track 'feat_type' in schema stage for automodel.fit() call.

- Finish the GridFS save and load of objects in vehicle.py, including both individual objects and all objects in the vehicle (load_all/flush).

- Implement "vehicle" object for metadata and the global model ensemble object to abstract python object storage/loading to/from the configued metadata collection.  Also provides a caching mechanism for objects already generated/loaded during the current run.  Represents the global state of an estimator.


- Update run_schema.py to allow selection of individual stages based on command line flags.




