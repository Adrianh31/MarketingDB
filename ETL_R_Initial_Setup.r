#########################################################################################################
# Name             : ETL_R_Initial_Setup
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

##_____________________________________________________________________________________________________________________________
# Load Packages
library(sqldf)
source('F:\\Analytics_Process\\R\\Custom_Functions\\LoopAppend.r')
##_____________________________________________________________________________________________________________________________
# Set Variables
WorkingDir = 'F://Marketing_FTP//'
ArchiveDir = 'Archive'
CustomerDir = 'Customers'
SalesDir = 'Sales'

##_____________________________________________________________________________________________________________________________
#create new database or if exists it will simply connect
setwd(WorkingDir)
db <- dbConnect(SQLite(), dbname="Marketing_SqlDB.sqlite")

# Setup archive directory and create if deleted
dir.create(file.path(WorkingDir, ArchiveDir), showWarnings = FALSE)

##_____________________________________________________________________________________________________________________________
# Read in Marketing Data
MarketingFilesDir <- paste(WorkingDir,CustomerDir,'//',sep='')

# Merge files in dir
df = LoopAppend(MarketingFilesDir,',',T,F)

# Insert Data into SQLlite
dbWriteTable(db, "MarketingData", df)

##_____________________________________________________________________________________________________________________________
# Read in Sales Data
SalesFilesDir <- paste(WorkingDir,SalesDir,'//',sep='')

# Merge files in dir
df = LoopAppend(SalesFilesDir,',',T,F)

# Insert Data into SQLlite
dbWriteTable(db, "SalesData", df)

##_____________________________________________________________________________________________________________________________
# Query Database
dbGetQuery(db, "SELECT 
                MD.CustomerAccount,
                MD.AccountStatus,
                MD.BusinessRep,
                SD.SalesID,
                SD.Product_Detail,
                SD.CostPerUnit
                FROM  
                MarketingData MD
                inner join SalesData SD on MD.CustomerAccount = SD.CustomerAccount")






















