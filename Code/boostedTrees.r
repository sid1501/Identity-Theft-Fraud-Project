#xgboost for identity theft detection
library(xgboost)
library(readr)
library(caret)
library(car)
library(stringr)

#load train, test and out of time data
train = data.frame(read_csv('training_data.csv'))
test = read_csv('testing_data.csv')
oot = read_csv('holdout_data.csv')

train_labels = as.numeric(train$fraud)
test_labels = as.numeric(test$fraud)

#selecting features to train model on
train_data = train[,-c(1,2,3)]
test_data = test[,-c(1,2,3)]


#convert to numeric df for xgboost (preparing training and testing data)
train_data[] <- lapply(train_data, as.numeric)
test_data[] <- lapply(test_data,as.numeric)
dtrain = xgb.DMatrix(data=data.matrix(train_data), label=train_labels)
dtest = xgb.DMatrix(data = data.matrix(test_data), label = test_labels)


#parameters for the model
params <- list(booster = "gbtree", objective = "binary:logistic", eta=0.3, gamma=0, max_depth=6, min_child_weight=1, subsample=1, colsample_bytree=1)

#ran to find the best number of iterations. 
xgbcv <- xgb.cv( params = params, data = dtrain, nrounds = 100, nfold = 5, showsd = T, print_every_n = 5, early_stop_round = 20, maximize = F)

#train the model 
xgb_model = xgb.train(params=params, data=dtrain, nrounds = 31, watchlist = list(val=dtest,train=dtrain),
                      print_every_n = 10, early_stop_round = 10, maximize = T , eval_metric = "auc")

#for TEST
xgbpred_test <- predict(xgb_model,dtest)
write_csv(cbind(test,xgbpred_test), 'pred_test.csv')
#for TRAIN
xgbpred_train <- predict(xgb_model,dtrain)
write_csv(cbind(train,xgbpred_train), 'pred_train.csv')
#prepare out of time data for model
oot_labels =as.numeric(oot$fraud)
oot_data = oot[,-c(1,2,3)]
oot_data[] =lapply(oot_data,as.numeric)
doot = xgb.DMatrix(data=data.matrix(oot_data), label=oot_labels)

#for OOT
xgbpred_oot <- predict(xgb_model, doot)
write_csv(cbind(oot,xgbpred_oot), 'pred_oot.csv')

### fdr calculator
fdr_Calculator <- function(base_df,pred_val){
  df=  cbind(base_df,pred_val)
  df=df[order(-pred_val),]
  fdr_lim = as.integer(nrow(df)*0.1)
  fdr_detect = df[1:fdr_lim,]
  caught=nrow(fdr_detect[fdr_detect$fraud==1,])
  tot=sum(base_df$fraud)
  return(caught/tot)
}

fdr_Calculator(test, xgbpred_test)
fdr_Calculator(train, xgbpred_train)
fdr_Calculator(oot, xgbpred_oot)
