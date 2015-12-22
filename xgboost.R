library(readr)
library(data.table)
library(xgboost)
set.seed(201)
library(caret)
cat("reading the train and test data\n")
#setwd("D:\\data\\final")
#the head prefix of different files since hive save all the database name as its prefix
train_pre <- "train_sample_twenty."
test_pre <- "test_sample_twenty."
submit_pre <- "submit."

train <- as.data.frame(fread("train_second.csv",na.strings = c("",NA,"NULL")))
test  <- as.data.frame(fread("test_second.csv",na.strings = c("",NA,"NULL")))
train <- setNames(train,substring(names(train),nchar(train_pre)+1))
test <- setNames(test,substring(names(test),nchar(test_pre)+1))

#for some may not exists in target;
test$target[test$target>0] <-1
test$target[is.na(test$target)] <-0
train$target[train$target>0] <-1
train$target[is.na(train$target)] <-0

#resample
#trainOne <- train[train$target==1,]
#trainZero <- train[train$target==0,]
#trainZero <- trainZero[sample(nrow(trainZero),124210),]
#train <- rbind(trainOne,trainZero)

#testOne <- test[test$target==1,]
#testZero <- test[test$target==0,]
#testZero <- testZero[sample(nrow(testZero),117420),]
#test <- rbind(testOne,testZero)


#get the names of columns except ID columns and target column
feature.names <- setdiff(names(train),c("adid","monitorid","userid","target"))
for (col in feature.names){
  print(col)
  train[[col]] <-as.numeric(train[[col]])
  test[[col]] <- as.numeric(test[[col]])
}

dval<-xgb.DMatrix(data=data.matrix(test[feature.names]),label=test$target,missing=NA)
dtrain<-xgb.DMatrix(data=data.matrix(train[feature.names]),label=train$target,missing=NA)
watchlist<-list(val=dval,train=dtrain)
param <- list(  objective           = "binary:logistic", 
                booster = "gbtree",
                eta                 = 0.05, # 0.06, #0.04,
                max_depth           = 4, #changed from default of 8:0.164560
                subsample           = 0.1, # 0.05
                colsample_bytree    = 0.5, # 0.9
                gamma               = 0.0,#0
                min_child_weight    = 4,#4
                max_delta_step      = 6,#1
                missing = NA
                #num_parallel_tree   = 2
 #               alpha = 0.1, 
  #              lambda = 0.5
)
clf <- xgb.train(   params              = param, 
                    data                = dtrain, 
                    nrounds             = 2000, 
                    verbose             = 0,
                    early.stop.round    = 100,
                    watchlist           = watchlist,
                    maximize            = FALSE,
                    eval_metric         = "logloss"
)
testRest <- predict(clf, data.matrix(test[feature.names]),missing = NA)
x <- testRest
x[x>0.3] <-1
x[x<1] <-0
print (sum(x))
confusionMatrix(x,test$target)
testy <- data.frame(monitorid=test$monitorid,userid=test$userid,target=x)
write_csv(testy, "test_predict.csv")

#predict
watchlist<-list(train=dval)
clf_pre <- xgb.train(   params              = param, 
                    data                = dval, 
                    nrounds             = 385, 
                    verbose             = 0,
                    early.stop.round    = 100,
                    watchlist           = watchlist,
                    maximize            = FALSE,
                    eval_metric         = "logloss"
                    #feval=Fmeature
)
submit <- as.data.frame(fread("submit_second.csv",na = c("",NA,"NULL")))
submit <- setNames(submit,substring(names(submit),nchar(submit_pre)+1))
for (col in feature.names){
  submit[[col]] <-as.numeric(submit[[col]])
}
submission <- data.frame(monitorid=submit$monitorid,userid=submit$userid)
submission$target <- NA 
numberRows <- nrow(submission)
iterationCount <-1
for (rows in split(1:nrow(submit), ceiling((1:nrow(submit))/10000))) {
  submission[rows, "target"] <- predict(clf_pre, data.matrix(submit[rows,feature.names]),missing = NA)
  print(paste(iterationCount*10000,numberRows,sep="/"))
  iterationCount <- iterationCount+1
}
cat("saving the submission file\n")
write_csv(submission, "submit-2015.12.18-385-0.05-6-best-0.1-sample.csv")
###########################################
#below is use for read split data in once
##########################################
#submission <- data.frame()
#for(i in 0:2){
#  tmp_submit <- as.data.frame(fread(paste("submit.csv",i,sep=""),na = c("",NA,"NULL")))
#  tmp_submit <- setNames(tmp_submit,substring(names(tmp_submit),nchar(submit_pre)+1))
#
#  for (col in feature.names){
#    tmp_submit[[col]] <-as.numeric(tmp_submit[[col]])
#  }
#  tmp_submission <- data.frame(monitorId=tmp_submit$monitorid,userid=tmp_submit$userid)
#  tmp_submission$target <- NA
#  numberRows <- nrow(tmp_submission)
#  iterationCount<- 1
#  for (rows in split(1:nrow(tmp_submit), ceiling((1:nrow(tmp_submit))/10000))) {
#    tmp_submission[rows, "target"] <- predict(clf_pre, data.matrix(tmp_submit[rows,feature.names]),missing = NA)
#    print(paste(i,paste(iterationCount*10000,numberRows,sep="/"),sep=" file: "))
#    iterationCount <- iterationCount+1
#  }
#  submission <- rbind(submission,tmp_submission)
#  rm(tmp_submission)
#  rm(tmp_submit)
#  gc()
#}
#subTmp <-  submission[submission$target>0.5,]
#cat("saving the submissi(n file\n")
#write_csv(subTmp, "submit-2015.12.2.csv")

