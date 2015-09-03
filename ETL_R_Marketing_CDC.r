#########################################################################################################
# Name             : ETL_R_Marketing_CDC
# Date             : 09-01-2015
# Author           : Christopher M
# Dept             : BEI
# Purpose          : Example of how to use R in a production environment for maintianing a marketing
#                    database
# Called by        : Not in production
#########################################################################################################
# ver    user        date(YYYYMMDD)        change  
# 1.0    w47593      20150901             initial
#########################################################################################################

# PLEASE READ
# In the NewCustomerRecords folder we have one new customer
# Customer 4578859 has been assigned a new business rep
# Customer 7729192 has been assigned a new business rep

##_____________________________________________________________________________________________________________________________
# Load Packages
library(sqldf)
source('F:\\Analytics_Process\\R\\Custom_Functions\\LoopAppend.r')

##_____________________________________________________________________________________________________________________________
# Set Variables
WorkingDir = 'F://Marketing_FTP//'
ArchiveDir = 'Archive'
CustomerDir = 'NewCustomerRecords'

##_____________________________________________________________________________________________________________________________
#create new database or if exists it will simply connect
setwd(WorkingDir)
db <- dbConnect(SQLite(), dbname="Marketing_SqlDB.sqlite")


##_____________________________________________________________________________________________________________________________
# Read in Marketing Data
MarketingFilesDir <- paste(WorkingDir,CustomerDir,'//',sep='')

# Merge files in dir
df = LoopAppend(MarketingFilesDir,',',T,F)


curruent_marketing_records <- dbGetQuery(db, "SELECT 
                                              *
                                              FROM  
                                              MarketingData ")
curruent_marketing_records$Active = NULL
curruent_marketing_records$ActiveEndDate = NULL

##_____________________________________________________________________________________________________________________________
# Extract records for update and new

# Extract records for update
BindedRecords <- rbind(df,curruent_marketing_records)
RecordsForUpdate <- unique(BindedRecords)
RecordsForUpdate <- sqldf("select CustomerAccount from RecordsForUpdate group by CustomerAccount having  count(*) > 1")

for(i in 1:nrow(RecordsForUpdate)){
UpdateQuery <- paste("Update MarketingData set Active = 0 , ActiveEndDate = ",paste("'",Sys.Date(),"'",sep=''),
                      "where CustomerAccount =",RecordsForUpdate[[1]][i]," and Active = 1")
dbGetQuery(db, UpdateQuery)
}

# Extract New Records
NewRecords <- sqldf("select 
                          tbl1.CustomerAccount
                          from df tbl1
                          left join curruent_marketing_records CMR on tbl1.CustomerAccount = CMR.CustomerAccount
                          where CMR.CustomerAccount IS NULL ")

 
##_____________________________________________________________________________________________________________________________
# Insert Data into Marketing table

InsertData <- rbind(NewRecords,RecordsForUpdate)
InsertData <- sqldf("Select * from df where CustomerAccount in (select CustomerAccount from InsertData)")

InsertData$Active = 1
InsertData$ActiveEndDate = '9999-01-01'

dbWriteTable(db, "MarketingData", InsertData,row.names=F,append = T)


##_____________________________________________________________________________________________________________________________
# Now Check our Marketing Table
dbGetQuery(db, "SELECT  * FROM  MarketingData order by CustomerAccount")


dbGetQuery(db, "SELECT 
                MD.Name as CustomerName,
                MD.CustomerAccount,
                MD.AccountStatus,
                MD.BusinessRep,
                SD.SalesID,
                SD.Product_Detail,
                SD.CostPerUnit
                FROM  
                MarketingData MD
                left join SalesData SD on MD.CustomerAccount = SD.CustomerAccount")


