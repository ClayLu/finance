library("RODBC")
library("lubridate")
library("plyr")
library("dplyr")
library("tidyr")
#library("tidyverse")
library("data.table")
library("TTR")
library("purrr")

#[設定資料日]==========================================================================================
#[1]讀檔
beta_today <- read.csv("D:\DSS\證券30日BETA\FILE\EDW_DSS_SPT_證券30日BETA_TDATE.csv", header = TRUE, sep = ",")
beta_today <- beta_today[1,1]

#[導入資料]==========================================================================================
#[1]讀檔
#BETA_ONE_1 <- read.csv("D:\EDW_DSS_SPT\證券30日BETA\FILE\EDW_DSS_SPT_證券30日BETA_DATA.csv", header = TRUE, sep = ",")

MSSQL_DW_SPT_CON <- read.csv("D:\DSS\conf\connection\MSSQL_DW_SPT.txt",header = TRUE,stringsAsFactors = FALSE ,sep = ",")
MSSQL_DW_SPT <- odbcConnect("MSSQL_DW_SPT", uid= MSSQL_DW_SPT_CON[1,1], pwd= rawToChar(base64enc::base64decode(MSSQL_DW_SPT_CON[1,2])) )
#MSSQL_DW_SPT <- odbcConnect("MSSQL_DW_SPT", uid= MSSQL_DW_SPT_CON[1,1], pwd= MSSQL_DW_SPT_CON[1,2])

#[[參數:日期]]
#EDW_SD_SOURCE_QUERY <- sprintf("SELECT [Biz_Id] AS AC , [Data_Dt] AS DATE , [Index_Value] AS RATIO from [MOD2].[FACT_SECS_ACCT_INDEX_SUM] where [Index_Id] = '029' and [Data_Dt] >= '%s' and [Data_Dt] <= '%s' " , (as.Date(as.character(ID_DATE) , "%Y%m%d") %m+% months(-30)) , (as.Date(as.character(ID_DATE) , "%Y%m%d")) )
BETA_ONE_1_QUERY <- sprintf( "select TDATE, STKNO , PRICE from [dbo].[FMT1101History] where TDATE > '%s' and PRICE >0" , format((as.Date(as.character(beta_today) , "%Y%m%d") %m+% months(-5)) ,"%Y%m%d") )
BETA_ONE_1 <- sqlQuery(MSSQL_DW_SPT, BETA_ONE_1_QUERY )
close(MSSQL_DW_SPT)

colnames(BETA_ONE_1) <- c('DTADTE' , 'STKNO' ,'LSTPRICE')
#若資料為西元記得拿掉
#BETA_ONE_1$DTADTE <- BETA_ONE_1$DTADTE + 19110000
#BETA_ONE_1 <- filter(BETA_ONE_1 , DTADTE > 20170701)
#==========================================================================================
#篩選交易日
MSSQL_BUSINESS_CON <- read.csv("D:\DSS\conf\connection\MSSQL_BUSINESS.txt",header = TRUE,stringsAsFactors = FALSE ,sep = ",")
MSSQL_BUSINESS <-odbcConnect("MSSQL_BUSINESS", uid= MSSQL_BUSINESS_CON[1,1], pwd= rawToChar(base64enc::base64decode(MSSQL_BUSINESS_CON[1,2])) )
#query時間和資料源時間一致
MSSQL_BUSINESS_QUERY <- sprintf("select TDATE_ from KGICALENDAR where HOLIDAY_= '' and TDATE_ > %s" , format((as.Date(as.character(beta_today) , "%Y%m%d") %m+% months(-5)) ,"%Y%m%d") )
T_DATE <- sqlQuery(MSSQL_BUSINESS, MSSQL_BUSINESS_QUERY )
T_DATE <- T_DATE$TDATE_
close(MSSQL_BUSINESS)

#都有交易的
BETA_ONE_w <- spread(BETA_ONE_1, STKNO, LSTPRICE) %>% arrange(desc(DTADTE) ) #tidyr
#考慮之後期間超過此2封關日
#BETA_ONE_w <- BETA_ONE_w[which(!BETA_ONE_w$DTADTE %in% c(20160204 ,20160205 , 20170126 , 20170125) ) , ]
#ifelse應只會回傳向量
BETA_ONE_w <- BETA_ONE_w[ which(BETA_ONE_w$DTADTE %in% T_DATE ) , ]
#==========================================================================================

colnames(BETA_ONE_w) <- trimws(colnames(BETA_ONE_w)) #欄位名稱有空白的處理

2018/2/8 處理只有一天
x_contain_0 <- as.vector(
which(apply(BETA_ONE_w, 2, function(x) sum(is.na(x)) == (nrow(BETA_ONE_w)-1) ) == TRUE)
)
BETA_ONE_0 <- cbind(BETA_ONE_w[ ,c(x_contain_0 )] , DTADTE =BETA_ONE_w[["DTADTE"]] )

if( ncol(BETA_ONE_0) > 1 )
{
BETA_0 <- BETA_ONE_0 %>% gather( STOCK , INDEX_VALUE , -DTADTE ) %>% filter(INDEX_VALUE != 0)
#BETA_0[which(BETA_0$STKNO %in% BETA_1$STKNO) , 'LSTPRICE'] <- 9999
BETA_0[ , 'INDEX_VALUE'] <- 9999
}else{
BETA_0 <- data.frame(DTADTE=integer() , STOCK=character() , INDEX_VALUE=numeric() )
}
#篩選當日
BETA_0 <- filter(BETA_0 , DTADTE == beta_today )

BETA_ONE_w <-BETA_ONE_w[ , c(setdiff(as.vector(which(apply(BETA_ONE_w, 2, function(x) TRUE ) )), x_contain_0) ) ]

#2018/2/7 報酬率計算
N_BETA <- nrow(BETA_ONE_w)
BETA_DATE <- BETA_ONE_w$DTADTE[1:(N_BETA - 1)]
BETA_ONE_tmp <- (BETA_ONE_w[1:(N_BETA - 1),] - BETA_ONE_w[2:N_BETA,]) / BETA_ONE_w[2:N_BETA,]
BETA_ONE_w <- BETA_ONE_tmp
rm(BETA_ONE_tmp)

BETA_ONE_w <- as.data.table(BETA_ONE_w)

x_contain <- as.vector(
which( apply(BETA_ONE_w , 2 , function(x) sum(is.na(x))==0 ) == TRUE )
)
BETA_ONE_w2 <- BETA_ONE_w[ , x_contain , with=FALSE ]
BETA_ONE_w3 <- BETA_ONE_w2

#計算SMA
SMA <- apply(BETA_ONE_w3[ , -1] ,2 , function(x) SMA(x , n= 30 )[30: nrow(BETA_ONE_w)] )
SMA2 <- data.table(SMA)

BETA <- sapply(colnames(SMA2) , function(x)
( cumsum( (BETA_ONE_w3[[x]][1:nrow(SMA2)]-SMA2[[x]])(BETA_ONE_w3[["TSEIDX"]][1:nrow(SMA2)]-SMA2[["TSEIDX"]]) )[30:nrow(SMA2) ]
- c( 0 , c(cumsum( (BETA_ONE_w3[[x]][1:nrow(SMA2)]-SMA2[[x]])(BETA_ONE_w3[["TSEIDX"]][1:nrow(SMA2)]-SMA2[["TSEIDX"]]) )[1:(nrow(SMA2)-30 ) ] ) )
)/
(cumsum( (BETA_ONE_w3[["TSEIDX"]][1:nrow(SMA2)]-SMA2[["TSEIDX"]])(BETA_ONE_w3[["TSEIDX"]][1:nrow(SMA2)]-SMA2[["TSEIDX"]]) )[30:nrow(SMA2) ]
- c( 0, cumsum( (BETA_ONE_w3[["TSEIDX"]][1:nrow(SMA2)]-SMA2[["TSEIDX"]])(BETA_ONE_w3[["TSEIDX"]][1:nrow(SMA2)]-SMA2[["TSEIDX"]]) )[1:(nrow(SMA2)-30 ) ] )
)
)

BETA <- as.data.table(BETA)
BETA[, DTADTE := BETA_DATE[ 1:(nrow(SMA2)-30+1 ) ] ] #新增欄位DTADTE
#BETA[ , "TSEIDX"] #檢查TSEIDX是否存在
BETA[ , c("TSEIDX") := NULL] #拿掉欄位TSEIDX
BETA <- filter(BETA , DTADTE == beta_today )

#transformer
BETA_gather <- BETA %>% gather(STOCK , BETA , -DTADTE )
colnames(BETA_gather) <- c("DTADTE", "STOCK" , "INDEX_VALUE")
#####################################################
#2018/2/7恢復日期
BETA_ONE_w[["DTADTE"]] <- BETA_DATE

x_contain_NA <- as.vector(
which( ( apply(BETA_ONE_w , 2 , function(x) !is.na(x[ which(BETA_ONE_w[["DTADTE"]] == beta_today) ] ) ) )
& ( apply(BETA_ONE_w , 2 , function(x) sum(is.na(x))>0 ) ) )
)

BETA_NA <- lapply(BETA_ONE_w[ ,x_contain_NA ,with=FALSE] ,
#BETA_NA_COUNT <- lapply(BETA_ONE_w[ ,x_contain_NA ,with=FALSE] , #驗證用
#X1 <- lapply(BETA_ONE_w[,c(2,3)] , #c(2:3) #驗證用
function(x){
xx<- data.frame(DTADTE = BETA_DATE , stock =x , TSEIDX_diff = c( (BETA_ONE_w3[["TSEIDX"]][1:nrow(SMA2)]-SMA2[["TSEIDX"]]) , rep(NA , nrow(BETA_ONE_w) - nrow(SMA2)) ) )
xx <- xx[which(!is.na(xx$stock)) ,] ####驗證用
rownames(xx) <- c(1:nrow(xx))
xx$DAY <- ifelse(c(nrow(xx):1 ) - 30 > 0 ,30 , c(nrow(xx):1 ) )
xx$stock_diff <- xx$stock - map2_dbl(x_start <- as.numeric(rownames(xx)) , y_end <- xx$DAY , function(x_start,y_end) sum( xx$stock[x_start: (x_start+y_end-1)] )/y_end )
xx <- xx[which(!is.na(xx$TSEIDX_diff)) ,] #避免此時有TSEIDX_diff為NA
xx$beta <- (cumsum(xx$stock_diffxx$TSEIDX_diff)[nrow(xx)] - c(0 , cumsum(xx$stock_diffxx$TSEIDX_diff)[-nrow(xx)]) ) /( cumsum(xx$TSEIDX_diffxx$TSEIDX_diff)[nrow(xx)] - c(0 , cumsum(xx$TSEIDX_diffxx$TSEIDX_diff)[-nrow(xx)]) )

                xx <- filter(xx , DTADTE == beta_today ) 
                
              }
)

2018/2/27預防BETA_NA可能為空
if(length(BETA_NA) > 0) {
BETA_NA_d <- ldply(BETA_NA, data.frame)[, c("DTADTE", ".id" , "beta")]
colnames(BETA_NA_d) <- c("DTADTE", "STOCK" , "INDEX_VALUE")
}else{
BETA_NA_d <- data.frame(DTADTE=integer() , STOCK=character() , INDEX_VALUE=numeric() )
}

#sum(is.na(BETA_NA_d$BETA) ) #驗證用
BETA_FINAL <- rbind(BETA_NA_d , BETA_gather)
rm(BETA_NA_d)
rm(BETA_gather)
#跟BETA_0合併
BETA_FINAL <- rbind(BETA_FINAL, BETA_0)

BETA_FINAL$INDEX_ID <- '002'
#寫檔==================================================================================================

write.csv(BETA_FINAL , "D:\DSS\證券30日BETA\FILE\EDW_DSS_SPT_證券30日BETA_OUTPUT.csv" , row.names = FALSE , quote = FALSE)

#===================================================================================================