#An unsegmented modelling approach was used for the stations in this binary classification model. This is applied to ensembe model techniques.
#To help stabilize the model we capture all variations in the variables over different times.

df_dev= sqlContext.sql("SELECT cause_woe, max_wind_gust_l30d,  max_dewpoint_tempf_l90d,  \
total_precipitation_l60d, total_snow_l7d, avg_humidity_l7d, avg_cloud_amount_daily, max_atm_pressure_l15d, \
target FROM Group_2.model_data_woe_median  where  substring(dateGMT, 1,4) in ('2016', '2018')")

df_test= sqlContext.sql("SELECT cause_woe, max_wind_gust_l30d,  max_dewpoint_tempf_l90d,  \
total_precipitation_l60d, total_snow_l7d, avg_humidity_l7d, avg_cloud_amount_daily, max_atm_pressure_l15d, \
target FROM Group_2.model_data_woe_median where  substring(dateGMT, 1,4) in ('2014', '2019')")

df_val= sqlContext.sql("SELECT cause_woe, max_wind_gust_l30d,  max_dewpoint_tempf_l90d,  \
total_precipitation_l60d, total_snow_l7d, avg_humidity_l7d, avg_cloud_amount_daily, max_atm_pressure_l15d, \
target FROM Group_2.model_data_woe_median where  substring(dateGMT, 1,4) in ('2015', '2017')")

df  = sqlContext.sql("SELECT cause_woe, max_wind_gust_l30d,  max_dewpoint_tempf_l90d, \
 total_precipitation_l60d, total_snow_l7d, avg_humidity_l7d, avg_cloud_amount_daily, \
 max_atm_pressure_l15d, target FROM Group_2.model_data_woe_median")

# rename target variable to 'label'
df = df.withColumnRenamed("target", "label")
df_dev = df_dev.withColumnRenamed("target", "label")
df_test = df_test.withColumnRenamed("target", "label")
df_val = df_val.withColumnRenamed("target", "label")

# add intercept column
from pyspark.sql.functions import lit
df = df.withColumn("mdl_intercept", lit(1))
df_dev = df_dev.withColumn("mdl_intercept", lit(1))
df_test = df_test.withColumn("mdl_intercept", lit(1))
df_val = df_val.withColumn("mdl_intercept", lit(1))

# set up modeling variables (including label)
modeling_vars = [
'cause_woe', 'max_wind_gust_l30d',  'max_dewpoint_tempf_l90d',  'total_precipitation_l60d', \
'total_snow_l7d', 'avg_humidity_l7d', 'avg_cloud_amount_daily', 'max_atm_pressure_l15d',  'label'
]

# Follow the Sample_ML example
from time import time
import numpy as np

# Import Logistic Regression Estimator
from pyspark.ml.classification import LogisticRegression

#Import Vector Assembler
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import (VectorSizeHint, VectorAssembler)

# Import Pipeline
from pyspark.ml import Pipeline

# Import the evaluation submodule
import pyspark.ml.evaluation as evals

# Import the tuning submodule
import pyspark.ml.tuning as tune

from pyspark.sql.functions import udf, col

# Create a LogisticRegression Estimator
lr = LogisticRegression(maxIter = 300)

# Create Pipeline (need to drop the label)
assembler = VectorAssembler(
    inputCols=df.select(modeling_vars).drop('label').columns,
    outputCol="features")
pipeline = Pipeline(stages=[assembler, lr])

# Create a BinaryClassificationEvaluator
evaluator = evals.BinaryClassificationEvaluator(metricName = 'areaUnderPR')

# Create the parameter grid
grid = tune.ParamGridBuilder()

# Add the hyperparameter
grid = grid.addGrid(lr.regParam, np.arange(0.00001, 1, 50))
# This does both l1 and l2 - list of 0 and 1
# NOTE - 1 = LASSO, 0 = Ridge regression
grid = grid.addGrid(lr.elasticNetParam, [0, 1])

# Build the grid
grid = grid.build()

# Create the CrossValidator
cv = tune.CrossValidator(estimator=pipeline, estimatorParamMaps=grid, evaluator=evaluator, numFolds=5)

#Here, we partition the data into X, Y training, validation and testing data. Data checks follow:
train = df_dev
test = df_test
valid = df_val


# Build the model
t0 = time()
logit_models = cv.fit(train)
tt = time() - t0
tt


# Follow the Sample_ML example
from time import time
import numpy as np

# Import Logistic Regression Estimator
from pyspark.ml.classification import LogisticRegression

#Import Vector Assembler
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import (VectorSizeHint, VectorAssembler)

# Import Pipeline
from pyspark.ml import Pipeline

# Import the evaluation submodule
import pyspark.ml.evaluation as evals

# Import the tuning submodule
import pyspark.ml.tuning as tune

from pyspark.sql.functions import udf, col

# Create a LogisticRegression Estimator
lr = LogisticRegression(maxIter = 300)

# Create Pipeline (need to drop the label)
assembler = VectorAssembler(
    inputCols=df.select(modeling_vars).drop('label').columns,
    outputCol="features")
pipeline = Pipeline(stages=[assembler, lr])

# Create a BinaryClassificationEvaluator
evaluator = evals.BinaryClassificationEvaluator(metricName = 'areaUnderPR')

# Create the parameter grid
grid = tune.ParamGridBuilder()

# Add the hyperparameter
grid = grid.addGrid(lr.regParam, np.arange(0.00001, 1, 50))

# NOTE - 1 = LASSO, 0 = Ridge regression
lr.elasticNetParam = 1
lr.maxiter = 200

# Build the grid
grid = grid.build()

# Create the CrossValidator
cv = tune.CrossValidator(estimator=pipeline, estimatorParamMaps=grid, evaluator=evaluator, numFolds=5)

#Here, we partition the data into X, Y training, validation and testing data. Data checks follow:
train = df_dev
test = df_test
valid = df_val


# Build the model
t0 = time()
logit_models = cv.fit(train)
tt = time() - t0
tt

logit_models



from time import time
import numpy as np

from pyspark.sql.functions import udf, col
from pyspark.ml.classification import RandomForestClassifier
#from hyperopt import fmin, tpe, hp, STATUS_OK, Trials


#Import Vector Assembler
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import (VectorSizeHint, VectorAssembler)

# Import Pipeline
from pyspark.ml import Pipeline

# Import the evaluation submodule
import pyspark.ml.evaluation as evals

# Import the tuning submodule
import pyspark.ml.tuning as tune

from pyspark.sql.functions import udf, col



# Create a Gradient Boosted Classifier
rdm = RandomForestClassifier()

# Create Pipeline (need to drop the label)
assembler = VectorAssembler(
    inputCols=df.select(modeling_vars).drop('label').columns,
    outputCol="features")
pipeline = Pipeline(stages=[assembler, rdm])

# Create a BinaryClassificationEvaluator
evaluator = evals.BinaryClassificationEvaluator(metricName = 'areaUnderPR')

# Create the parameter grid
grid = tune.ParamGridBuilder()

# Add the hyperparameter
#grid = grid.addGrid(rdm.featureSubsetStrategy, ["2",  "7",  "12", "18", "23", "28", "33", "39", "44", "49"])
grid = grid.addGrid(rdm.featureSubsetStrategy, ["2", "7", "12", "18", "23", "29", "34", "40", "45", "51"])
#grid = grid.addGrid(rdm.maxDepth, [5,10])
grid = grid.addGrid(rdm.numTrees, [50, 162, 275, 387, 500])
grid = grid.addGrid(rdm.minInstancesPerNode, [634, 8407, 16181, 23954, 31728])




# Build the grid
grid = grid.build()

# Create the CrossValidator
cv = tune.CrossValidator(estimator=pipeline, estimatorParamMaps=grid, evaluator=evaluator, numFolds=5, parallelism = 12)

#Here, we partition the data into X, Y training, validation and testing data. Data checks follow:
train = df_dev
test = df_test
valid = df_val


# Build the model
t0 = time()
gbt_models = cv.fit(train)
tt = time() - t0
tt


np.linspace(start = 50, stop = 500, num = 5)

from time import time
import numpy as np

from pyspark.sql.functions import udf, col
from pyspark.ml.classification import GBTClassifier


#Import Vector Assembler
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import (VectorSizeHint, VectorAssembler)

# Import Pipeline
from pyspark.ml import Pipeline

# Import the evaluation submodule
import pyspark.ml.evaluation as evals

# Import the tuning submodule
import pyspark.ml.tuning as tune

from pyspark.sql.functions import udf, col

# Create a Gradient Boosted Classifier
gbt = GBTClassifier()

# Create Pipeline (need to drop the label)
assembler = VectorAssembler(
    inputCols=df.select(modeling_vars).drop('label').columns,
    outputCol="features")
pipeline = Pipeline(stages=[assembler, gbt])

# Create a BinaryClassificationEvaluator
evaluator = evals.BinaryClassificationEvaluator(metricName = 'areaUnderPR')

# Create the parameter grid
grid = tune.ParamGridBuilder()

# Add the hyperparameter
grid = grid.addGrid(gbt.featureSubsetStrategy, ["2",  "7", "12", "18", "23", "28", "33", "39", "44", "49"])
grid = grid.addGrid(gbt.maxDepth, [5,10])
#grid = grid.addGrid(gbt.numTrees, [50, 163, 275, 388, 500])



# Build the grid
grid = grid.build()

# Create the CrossValidator
cv = tune.CrossValidator(estimator=pipeline, estimatorParamMaps=grid, evaluator=evaluator, numFolds=5, parallelism = 8)

#Here, we partition the data into X, Y training, validation and testing data. Data checks follow:
train = df_dev
test = df_test
valid = df_val


# Build the model
t0 = time()
gbt_models = cv.fit(train)
tt = time() - t0
tt


# Follow the Sample_ML example
from time import time
import numpy as np

# Import Logistic Regression Estimator
from pyspark.ml.classification import LogisticRegression

#Import Vector Assembler
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import (VectorSizeHint, VectorAssembler)

# Import Pipeline
from pyspark.ml import Pipeline

# Import the evaluation submodule
import pyspark.ml.evaluation as evals

# Import the tuning submodule
import pyspark.ml.tuning as tune

from pyspark.sql.functions import udf, col

# Create a LogisticRegression Estimator
lr = LogisticRegression(maxIter = 300)

# Create Pipeline (need to drop the label)
assembler = VectorAssembler(
    inputCols=df.select(modeling_vars).drop('label').columns,
    outputCol="features")
pipeline = Pipeline(stages=[assembler, lr])

# Create a BinaryClassificationEvaluator
evaluator = evals.BinaryClassificationEvaluator(metricName = 'areaUnderPR')

# Create the parameter grid
grid = tune.ParamGridBuilder()

# Add the hyperparameter
grid = grid.addGrid(lr.regParam, np.arange(0.00001, 1, 50))
# NOTE - 1 = LASSO, 0 = Ridge regression
lr.elasticNetParam = 0
lr.maxiter = 300

# Build the grid
grid = grid.build()

# Create the CrossValidator
cv = tune.CrossValidator(estimator=pipeline, estimatorParamMaps=grid, evaluator=evaluator, numFolds=5)

train = df_dev
test = df_test
valid = df_val



# Build the model
t0 = time()
logit_models = cv.fit(train)
tt = time() - t0
tt

lr.elasticNetParam = 1
lr.maxiter = 300



# Model Evaluations

dir(logit_models)

best_lr = logit_models.bestModel
test_results = best_lr.transform(valid)
# test_results.show()
# Evaluate the predictions
print(evaluator.evaluate(test_results))

import matplotlib.pyplot as plt
import seaborn
from sklearn.metrics import roc_auc_score, roc_curve, precision_recall_curve, average_precision_score
from pyspark.sql.functions import col,column, udf
from pyspark.sql.types import *

def to_array(col):
    def to_array_(v):
        return v.toArray().tolist()
    return udf(to_array_, ArrayType(DoubleType()))(col)

test_results = test_results.withColumn('prob', to_array(col('probability')).getItem(1))

test_results_pd = test_results.toPandas()

lasso_precision, lasso_recall, _ = precision_recall_curve(test_results_pd['label'], test_results_pd['prob'])
lasso_avg_pr = average_precision_score(test_results_pd['label'], test_results_pd['prob'])

plt.figure()
plt.plot(lasso_recall, lasso_precision,
         label='LASSO (Avg Precision = %0.4f)' % lasso_avg_pr,
         linewidth=0.5)
plt.xlabel('Recall')
plt.ylabel('Precision')
plt.ylim([0.0, 0.20])
plt.xlim([0.0, 1.0])
plt.title('Precision-Recall Curve'.format(
          lasso_avg_pr))
plt.legend(loc="upper right")
display(plt.show())


lasso_roc = roc_auc_score(test_results_pd['label'], test_results_pd['prob'])
lasso_fpr, lasso_tpr, lasso_thresh = roc_curve(test_results_pd['label'], test_results_pd['prob'])


plt.figure()
plt.plot(lasso_fpr, lasso_tpr,
         label='LASSO (area = %0.4f)' % lasso_roc,
         linewidth=0.5)
plt.plot([0, 1], [0, 1],'k--')
plt.xlim([0.0, 1.0])
plt.ylim([0.0, 1.05])
plt.xlabel('False Positive Rate')
plt.ylabel('True Positive Rate')
plt.title('ROC Curve on Holdout Data')
plt.legend(loc="lower right")
display(plt.show())
