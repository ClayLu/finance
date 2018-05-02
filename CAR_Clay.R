#getwd()
#setwd("C:\\RProject\\CAR")
ti_train <- read.csv("C:\\RProject\\CAR\\trainset.csv" ,encoding="latin1" , sep = "," ,stringsAsFactors = FALSE) 

#"utf-8" )

#manupulation row 1020 and 1433
a <- which( is.na(ti_train[ ,22]) )
library(stringr)
ti_train[a ,5:22] <- ti_train[a ,4:21]
for(i in 1:length(a)){
ti_train[a[i] ,4 ] <-  str_split(ti_train[a[i] ,3 ], ",")[[1]][2]
ti_train[a[i] ,3 ] <-  str_split(ti_train[a[i] ,3 ], ",")[[1]][1]
}

#ti_train$name <- as.character(ti_train$name)
ti_train$price  <- as.numeric(ti_train$price)

library("lubridate")
#ti_train$dateCrawled <-  ymd_hms(ti_train$dateCrawled)
ti_train$dateCreated <-  ymd_hms(ti_train$dateCreated)
ti_train$lastSeen <-  ymd_hms(ti_train$lastSeen)
ti_train$SurviveTime <- as.numeric(ti_train$lastSeen - ti_train$dateCreated )


ti_train$seller <- as.factor(ti_train$seller)
ti_train$offerType <- as.factor(ti_train$offerType)
ti_train$abtest <- as.factor(ti_train$abtest)
ti_train$vehicleType <- as.factor(ti_train$vehicleType)
#ti_train$yearOfRegistration <- ordered(as.factor(ti_train$yearOfRegistration) )
ti_train$yearOfRegistration <- as.numeric(ti_train$yearOfRegistration) 
ti_train$gearbox <- as.factor(ti_train$gearbox)
ti_train$powerPS <- as.numeric(ti_train$powerPS)
ti_train$model <- as.factor(ti_train$model)
ti_train$kilometer <- as.numeric(ti_train$kilometer)
ti_train$monthOfRegistration <- as.factor(ti_train$monthOfRegistration)
ti_train$fuelType <- as.factor(ti_train$fuelType)
ti_train$brand <- as.factor(ti_train$brand)
a <- levels(ti_train$brand)
a[length(a)+1] <- 'lada'
ti_train$brand <- factor(ti_train$brand , a)
ti_train$notRepairedDamage <- as.factor(ti_train$notRepairedDamage)
ti_train$ad_exist_time <- as.numeric(ti_train$ad_exist_time)


#delete zero variance variable
a1 <- c('nrOfPictures', 'seller' , 'offerType' )
for(i in 1:length(a1)){
a <- which( names(ti_train) == a1[i])
ti_train <- ti_train[,-a]
}

a[length(a)+1] <- 'other'
#brand group count
ti_train$brand_group <- ti_train$brand
#tr_brand <- ti_train %>%  group_by( brand) %>% summarise ( count = n()) 
ti_train$brand_group <- factor(ti_train$brand_group , a)
tr_brand_h <-  ti_train %>%  group_by( brand) %>% summarise ( count = n()) %>% filter( count > 30)
for(i in 1:5000){
  if(length(setdiff( ti_train[i,'brand_group']  , tr_brand_h$brand  )) > 0) 
  { ti_train[i,'brand_group'] <- 'other'
    #print(ti_train[i,'brand_group'] )
    }
}
#which(ti_train$brand == 'daewoo')
#ti_train[795 , "brand_group"]  



#library(rpart)
model_car <- rpart(price ~ vehicleType + yearOfRegistration +gearbox +model  +powerPS +kilometer  +fuelType +brand + notRepairedDamage + ad_exist_time + SurviveTime , ti_train)
#plot(model_car) ; text(model_car)

#lm , no variable model
model_car <- lm(price ~ vehicleType + yearOfRegistration +gearbox   +powerPS +kilometer  +fuelType +brand + notRepairedDamage + ad_exist_time + SurviveTime , ti_train)
#log lm
#model_car <- lm(log(price) ~ vehicleType + yearOfRegistration +gearbox   +powerPS +kilometer  +fuelType +brand + notRepairedDamage + ad_exist_time + SurviveTime , ti_train)

#Poission reg
#62
model_car <- glm(price ~ vehicleType + yearOfRegistration +gearbox   +powerPS +kilometer  +fuelType +brand_group + notRepairedDamage + ad_exist_time + SurviveTime , ti_train ,family = poisson())
#final model
model_car <- glm(price ~ vehicleType + yearOfRegistration +gearbox   +powerPS +kilometer  +fuelType +brand_group + notRepairedDamage + SurviveTime , ti_train ,family = poisson())
#model_car <- glm(price ~ vehicleType + yearOfRegistration +gearbox   +powerPS +kilometer  +fuelType +brand + notRepairedDamage + ad_exist_time + SurviveTime , ti_train ,family = poisson())
model_car <- glm(price ~ vehicleType + +model +yearOfRegistration +gearbox   +powerPS +kilometer  +fuelType +brand + notRepairedDamage + ad_exist_time + SurviveTime , ti_train ,family = poisson())
#rsq.rpart(model_car)
library(e1071)
model_car <- svm(price ~ vehicleType + yearOfRegistration +gearbox   +powerPS +kilometer  +fuelType +brand_group + notRepairedDamage + SurviveTime , ti_train )



#以下做了資料標準化，但final model並未用到
ti_train_s <- ti_train
ti_train_s$SurviveTime <- (ti_train_s$SurviveTime - min(ti_train_s$SurviveTime) )/ (max(ti_train_s$SurviveTime) - min(ti_train_s$SurviveTime))
ti_train_s$ad_exist_time <- (ti_train_s$ad_exist_time - min(ti_train_s$ad_exist_time) ) / (max(ti_train_s$ad_exist_time)- min(ti_train_s$ad_exist_time) ) 
ti_train_s$kilometer <- ( ti_train_s$kilometer - min(ti_train_s$kilometer)) / (max(ti_train_s$kilometer) - min(ti_train_s$kilometer))
ti_train_s$powerPS <- (ti_train_s$powerPS -min(ti_train_s$powerPS) )/ (max(ti_train_s$powerPS) -min(ti_train_s$powerPS))
ti_train_s$yearOfRegistration <- (ti_train_s$yearOfRegistration -min(ti_train_s$yearOfRegistration) )/(max(ti_train_s$yearOfRegistration) -min(ti_train_s$yearOfRegistration))
ti_train_s$brand <- factor(ti_train_s$brand , a)
ti_train_s$brand_model <- as.factor(( paste0(ti_train_s$brand ,'_' ,ti_train_s$model) ))

model_car_s <- glm(price ~ vehicleType  +yearOfRegistration +gearbox   +powerPS +kilometer  +fuelType +brand + notRepairedDamage + ad_exist_time + SurviveTime , ti_train_s ,family = poisson())
model_car_s <- lm(price ~ vehicleType + yearOfRegistration +gearbox   +powerPS +kilometer  +fuelType +brand + notRepairedDamage + ad_exist_time + SurviveTime , ti_train_s)
model_car_s$xlevels[["brand"]] <- union(model_car_s$xlevels[["brand"]] , levels(ti_test_s$brand))
model_car_s <- svm(price ~ vehicleType  +yearOfRegistration +gearbox +powerPS +kilometer  +fuelType   , ti_train_s )


#載入testset
ti_test <- read.csv("C:\\RProject\\CAR\\testset.csv" ,encoding="latin1" , sep = "," ,stringsAsFactors = FALSE) 

#"utf-8" )

#manupulation row 1020 and 1433
a <- which( is.na(ti_test[ ,21]) )
library(stringr)
ti_test[a ,5:21] <- ti_test[a ,4:20]
for(i in 1:length(a)){
  ti_test[a[i] ,4 ] <-  str_split(ti_test[a[i] ,3 ], ",")[[1]][2]
  ti_test[a[i] ,3 ] <-  str_split(ti_test[a[i] ,3 ], ",")[[1]][1]
}

#ti_test$name <- as.character(ti_test$name)
#ti_test$price  <- as.numeric(ti_test$price)

library("lubridate")
ti_test$dateCrawled <-  ymd_hms(ti_test$dateCrawled)
ti_test$dateCreated <-  ymd_hms(ti_test$dateCreated)
ti_test$lastSeen <-  ymd_hms(ti_test$lastSeen)
ti_test$SurviveTime <- as.numeric(ti_test$lastSeen - ti_test$dateCreated )


ti_test$seller <- as.factor(ti_test$seller)
ti_test$offerType <- as.factor(ti_test$offerType)
ti_test$abtest <- as.factor(ti_test$abtest)
ti_test$vehicleType <- as.factor(ti_test$vehicleType)
#ti_test$yearOfRegistration <- ordered(as.factor(ti_test$yearOfRegistration) )
ti_test$yearOfRegistration <- as.numeric(ti_test$yearOfRegistration) 
ti_test$gearbox <- as.factor(ti_test$gearbox)
ti_test$powerPS <- as.numeric(ti_test$powerPS)
ti_test$model <- as.factor(ti_test$model)
ti_test$kilometer <- as.numeric(ti_test$kilometer)
ti_test$monthOfRegistration <- as.factor(ti_test$monthOfRegistration)
ti_test$fuelType <- as.factor(ti_test$fuelType)
ti_test$brand <- as.factor(ti_test$brand)
ti_test$notRepairedDamage <- as.factor(ti_test$notRepairedDamage)
ti_test$ad_exist_time <- as.numeric(ti_test$ad_exist_time)

ti_test$fuelType <- factor(ti_test$fuelType , levels(ti_train$fuelType))


#delete zero variance variable
a1 <- c('nrOfPictures', 'seller' , 'offerType' )
for(i in 1:length(a1)){
  a <- which( names(ti_test) == a1[i])
  ti_test <- ti_test[,-a]
}



ti_test$brand_group <- ti_test$brand
a <- levels(ti_train$brand)
a[length(a)+1] <- 'other'
ti_test$brand_group <- factor(ti_test$brand_group , a)
for(i in 1:5000){
  if(length(setdiff( ti_test[i,'brand_group']  , tr_brand_h$brand  )) > 0) 
  { ti_test[i,'brand_group'] <- 'other'
  #print(ti_train[i,'brand_group'] )
  }
}

model_car$xlevels[["brand"]] <- union(model_car$xlevels[["brand"]] , levels(ti_test$brand))
#if model was build
a11 <- predict(model_car , ti_test)
#a11 <- exp(a11)
a11 <- cbind( ti_test$id ,a11 )
colnames(a11) <- c('id' , 'predict')
write.csv(a11, file = "C:\\RProject\\CAR\\submit.csv", row.names = FALSE ,col.names = TRUE ,quote =FALSE)



ti_test_s <- ti_test
ti_test_s$SurviveTime <- (ti_test_s$SurviveTime - min(ti_test_s$SurviveTime) )/ (max(ti_test_s$SurviveTime) - min(ti_test_s$SurviveTime))
ti_test_s$ad_exist_time <- (ti_test_s$ad_exist_time - min(ti_test_s$ad_exist_time) ) / (max(ti_test_s$ad_exist_time)- min(ti_test_s$ad_exist_time) ) 
ti_test_s$kilometer <- ( ti_test_s$kilometer - min(ti_test_s$kilometer)) / (max(ti_test_s$kilometer) - min(ti_test_s$kilometer))
ti_test_s$powerPS <- (ti_test_s$powerPS -min(ti_test_s$powerPS) )/ (max(ti_test_s$powerPS) -min(ti_test_s$powerPS))
ti_test_s$yearOfRegistration <- (ti_test_s$yearOfRegistration -min(ti_test_s$yearOfRegistration) )/(max(ti_test_s$yearOfRegistration) -min(ti_test_s$yearOfRegistration))
ti_test_s$brand_model <- as.factor(( paste0(ti_test_s$brand ,'_' ,ti_test_s$model) ))
ti_test_s$fuelType <- factor(ti_test_s$fuelType , levels(ti_train_s$fuelType))

a11_s <- predict(model_car_s , ti_test_s)
#a11 <- exp(a11)
a11_s <- cbind( ti_test_s$id ,a11_s )
colnames(a11_s) <- c('id' , 'predict')
write.csv(a11_s, file = "C:\\RProject\\CAR\\submit.csv", row.names = FALSE ,col.names = TRUE ,quote =FALSE)
