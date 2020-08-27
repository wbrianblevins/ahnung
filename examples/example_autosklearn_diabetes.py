#!/usr/bin/env python

import time

import autosklearn.classification
import sklearn.model_selection
import sklearn.datasets
import sklearn.metrics


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
allowed_jobs = 4

#
# Select overall max time in seconds and per model max time in seconds.
#
# time_left_for_this_taskint, optional (default=3600)
max_time_global = 360
# per_run_time_limitint, optional (default=360)
max_time_model = 36

print("Selected estimators      : ", use_est)
print("Deselected estimators    : ", exc_est)
print("Selected preprocessors   : ", use_preproc)
print("Deselected preprocessors : ", exc_preproc)
print("Max time global (sec)    : ", max_time_global)
print("Max time per model (sec) : ", max_time_model)
print("CPUs Used                : ", allowed_jobs)

print("Loading Diabetes dataset.")
X, y = sklearn.datasets.load_diabetes(return_X_y=True)

print("Splitting out test and train datasets.")
X_train, X_test, y_train, y_test = \
        sklearn.model_selection.train_test_split(X, y, random_state=1)

print("Instantiating AutoSklearnClassifier.")
automl = autosklearn.classification.AutoSklearnClassifier(
             time_left_for_this_task = max_time_global,
             per_run_time_limit      = max_time_model,
             include_estimators      = use_est,
             exclude_estimators      = exc_est,
             include_preprocessors   = use_preproc,
             exclude_preprocessors   = exc_preproc,
             n_jobs                  = allowed_jobs )

print("Training the AutoSklearnClassifier on the Diabetes train dataset.")
automl.fit(X_train, y_train)

end_wc_seconds = time.time()

print("Checking the AutoSklearnClassifier against the Diabetes test dataset.")
y_hat = automl.predict(X_test)

print("\n\tElapsed time (sec): ", end_wc_seconds - start_wc_seconds)
print("Accuracy score: ", sklearn.metrics.accuracy_score(y_test, y_hat))

# print("\n\tResults DataFrame: ")
# print(automl.cv_results_)

# print("\n\tStatistics: ")
# print(automl.sprint_statistics())

# print("\n\tMODELS: ")
# print(automl.show_models())


