library(readr)
library(h2o)
library(data.table)
library(caret)
set.seed(123)

cat("reading the train and test data\n")
train <- as.data.frame(fread("train.csv",na.strings = c("",NA,"NULL")))
test  <- as.data.frame(fread("test.csv",na.strings = c("",NA,"NULL")))

test$target[test$target>0] <-1
test$target[is.na(test$target)] <-0

train$target[train$target>0] <-1
train$target[is.na(train$target)] <-0

#fill na in train and test
train[is.na(train)] <-0
test[is.na(test)] <-0

#get the names of columns except ID columns and target column
feature.names <- setdiff(names(train),c("adid","monitorid","userid","target"))
#transform the type of the dataset
for (col in feature.names){
  print(col)
  train[[col]] <-as.numeric(train[[col]])
  test[[col]] <- as.numeric(test[[col]])
}

h2o.init(nthreads=-1,max_mem_size='60G')
trainHex<-as.h2o(train)
clf <- h2o.randomForest(x=feature.names,
                          y="target", 
                          ntrees = 500,
                          max_depth = 30,
                          nbins_cats = 500, 
                          training_frame=trainHex)
testHex<-as.h2o(test)
testRest<-as.data.frame(h2o.predict(clf,testHex))[,1]
x <- testRest
x[x>0.3] <-1
x[x<1] <-0
print (sum(x))
confusionMatrix(x,test$target)
testy <- data.frame(monitorid=test$monitorid,userid=test$userid,target=x)
write_csv(testy, "test_predict.csv")

clf_pre <- h2o.randomForest(x=feature.names,
                        y="target", 
                        ntrees = 500,
                        max_depth = 30,
                        nbins_cats = 500, ## allow it to fit store ID
                        training_frame=testHex)

submit <- as.data.frame(fread("submit.csv",na = c("",NA,"NULL")))
submit$target <-NA
setnames(submit,names(train))

#transform the type of dataset
for (col in feature.names){
  submit[[col]] <-as.numeric(submit[[col]])
}

#fill na
submit[is.na(submit)] <-0

#read file
submission <- data.frame(monitorid=submit$monitorid,userid=submit$userid)

submission$target <- NA 
#predict
submission$target <- as.data.frame(predict(clf_pre, as.h2o(submit)))[,1]

cat("saving the submission file\n")
write_csv(submission, "rf.csv")
