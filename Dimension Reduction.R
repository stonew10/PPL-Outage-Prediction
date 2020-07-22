#Correlation matrix calculations
#All features were called into 'features' with non-numerical variables like latitude, GMT, etc. were removed.
#Correlation was calculated for the whole feature set.
#With a univariate correlation of 0.005 and 0.9, pairwise comparisons of the correlation produced 26 model variables.

library(SparkR)
features <- sql("SELECT * FROM Group_2.All_Features")
library(sparklyr)
library(dplyr)
library(Rcpp)
library(car)
sc <- spark_connect(method = "databricks")
features <- sdf_sql(sc, "SELECT * FROM Group_2.model_data_woe_median")
features


f_cols = colnames(select(features, 2:121))
features2 = features %>% select (f_cols)

univariate_correlation = 0.005
correlation_thresh = 0.9
target <- 'target'

cor_mat = ml_corr(features2 , method= "spearman")
cor_mat_abs =  mutate_all(cor_mat, funs(abs))


correl = cor_mat_abs[nrow(cor_mat_abs),]
correl = correl[ , colSums(is.na(correl)) == 0]
correl= cbind(correl[,colSums(correl)> univariate_correlation])
correl = correl[,colSums(correl)< 0.99]


mvar0 = c(colnames(correl),target)
xtr2 = features2 %>% select(mvar0)
cor_mat2 = ml_corr(xtr2, method= "spearman")
cor_mat_abs2 =  mutate_all(cor_mat2, funs(abs))
mcorr = list()
ctr = 1
for( i in 1: ncol(correl)){
  for (j in i:ncol(correl)) {
    if(cor_mat_abs2[i,j] > correlation_thresh && j>i ) {
     if (correl[1,i]>correl[1,j]) {mcorr[[ctr]] = colnames(correl)[[j]]
                                   ctr = ctr + 1} else {
                                   mcorr[[ctr]] = colnames(correl)[[i]]
                                   ctr = ctr + 1       }

                                                    } else { ctr = ctr}

                               }
                           }
mcorr = unique(unlist(mcorr))
model_vars= colnames(cor_mat_abs2[,!(names(cor_mat_abs2) %in% mcorr)])
model_vars


features_sel <- sdf_sql(sc, "SELECT cause_woe, max_cloud_amount_daily, max_precipitation_daily,
                              avg_wind_gust_daily, max_wind_gust_l30d, max_dewpoint_tempf_l90d,
                              max_precipitation_l7d, max_precipitation_l30d, max_atm_pressure_l15d,
                            total_snow_l7d, avg_snow_l7d, total_precipitation_l7d, total_precipitation_l30d,
                            avg_wind_gust_l90d, total_snow_daily, avg_cloud_amount_daily, max_humidity_l90d,
                            max_precipitation_l3d, max_precipitation_l15d, max_precipitation_l60d, total_snow_l3d,
                            total_snow_l90d, total_precipitation_l3d, total_precipitation_l15d, total_precipitation_l60d,
                                avg_humidity_l90d, target

FROM Group_2.model_data_woe_median")


features_sel2 <- data.frame(features_sel)
M <- cor(features_sel2)
library(corrplot)
corrplot(M, method = 'shade')
