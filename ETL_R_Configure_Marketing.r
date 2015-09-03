#########################################################################################################
# Name             : ETL_R_Configure_Marketing
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

##_____________________________________________________________________________________________________________________________
# Set Variables
WorkingDir = 'F://Marketing_FTP//'
ArchiveDir = 'Archive'

##_____________________________________________________________________________________________________________________________
#create new database or if exists it will simply connect
setwd(WorkingDir)
db <- dbConnect(SQLite(), dbname="Marketing_SqlDB.sqlite")


dbGetQuery(db, "SELECT 
                *
                FROM  
                MarketingData ")

##_____________________________________________________________________________________________________________________________
# Now after our initial import we will add a new columns to the marketing db to track changes

dbGetQuery(db, "ALTER TABLE MarketingData ADD COLUMN Active int ")
dbGetQuery(db, "ALTER TABLE MarketingData ADD COLUMN ActiveEndDate date ")


dbGetQuery(db, "Update MarketingData set Active = 1 ")
dbGetQuery(db, "Update MarketingData set ActiveEndDate = '9999-01-01' ")


##_____________________________________________________________________________________________________________________________
# We can see that all Marketing Rows are active

dbGetQuery(db, "SELECT 
                *
                FROM  
                MarketingData ")














