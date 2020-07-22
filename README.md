# PPL-Outage-Prediction

Using data given by PPL (an energy company in Pennsylvania), databases are combined, exploratory data analysis is run, and models are created to predict which stations are likely to have an outage occur within one day


**Problem Description**:

PPL utilities, a regulated public utility company with $11.9 Billion in assets delivers electricity to approximately 1.4 million customers in 29 counties of eastern and central Pennsylvania. According to the U.S. Energy Information Administration (EIA), in 2016, customers experienced an average of 1.3 power interruptions and went an average of four hours without power in a year. As of March 15, 2020, there were 660 outages in Pennsylvania with PPL Utilities grid accounting for 22% of them. Given the need to improve customer experience and service reliability, this project is geared towards the use of historic data on measurable external factors and service outage provided by PPL Utilities to build a classification model to help predict the possibility of an outage given certain conditions.


**Exploratory Data Analysis**:

PPL provided two tables, weather and outage data. The outage data contains 325,851 rows spaning January 2014 to June 2019. This provides critical information on outage status, incident status, time of the incident, time of incident restoration, location description, and cause of outage which are important to our analysis. As shown in the target dat prep section, this has been joined with the weather table using the longitudes and latitudes to help ascertain the effects of snowfall and wind speed. The weather table provides hourly information on air temperature, dew point temperature, wind speed, wind gust, wind direction, snowfall and leaf coverage among others.
51% of the data shows no outage and 49% shows power restoration. A closer look at the restored incidents shows that majority of outages are caused by Unknown factors, Trees- Not Trimming Related, equipment failures, animals and scheduled outages. There are usually spikes in outages between the months of May and July. 2019 was quite different and 2018 also showed some spikes in March. Unknown issues began increasing from January 2016 with a count of 610 incidents and peaked in March 2019 with 2,876 incidents before beginning to decline. Trees-Not Trimming Related, which historically had less than 1,000 incidents also spiked in March 2019 with 2,238 incidents and can be treated as an outlier. Notably, the number of incidents decreased substaintially in 2019.


**Feature Engineering**:

With the aim to get the underlying weather conditions that lead to outage, 9 features from weather table were transformed into 137 features to find important features; these features were created by producing lags, maxes, averages, and sums of the original 9 attributes. These include wind gust, cloud amount, relative humidity, dewpoint temperature, air temperature, precipitation, snow fall, wind speed and atmospheric pressure. To get the most dire levels that lead to outage, a summation was taken of snowfall and precipitation on an hourly basis, averaged the max levels for the rest of the features on a daily basis.
All features were called into 'features' with non-numerical variables like latitude, GMT, etc. were removed. Correlation was calculated for the whole feature set. With a univariate correlation of 0.005 and 0.9, pairwise comparisons of the correlation produced 26 model variables.


**Model Development**:

An unsegmented modelling approach was used for the stations in this binary classification model. This is applied to ensembe model techniques. As the dataset is balanced, SMOTE was not conducted and undersampling techniques have been employed prior to developing the model. Below is the process followed:
  * Data integrity check
  * Event rate calculation: 70% training data, 30% validation data).
  * Variable treatment and variable derivation to be carried on the development data.
  * Univariates to be obtained and Missing Value Imputation (MVI) to be carried out.
  * Information value (IV) to be calculated for all variables after MVI and the variables with IV < 0.02 to be eliminated.
  * The variable bivariates to be obtained and categorical variables to be replaced with the corresponding WOE.
  * Post clustering, VIF values to be calculated and variables with VIF >2 (multicollinearity) to be eliminated.
  * Thereafter, Proc logistic (stepwise) to be used to shorten the list of variables. Different Proc Logistic iterations to be conducted to get the best-fit model.
  * Once the model is finalised, the model is validated on the 'hold out sample' and the 'out of time' data to check the stability.
  * Later, other machine learning techniques will be conducted to find the best model (most likely XGBoost) with highest accuracy / ROC.
  
