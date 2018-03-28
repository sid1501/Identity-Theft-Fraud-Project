setwd("~/Documents/Fraud Analytics/Project 2")

install.packages('DBI')
install.packages('RSQLite')
install.packages('lubridate')

library(DBI)
library(RSQLite)
library(lubridate)


data = read.csv('applications.csv')

data$date = mdy(data$date)
View(data)

colnames(data)[1] = 'record'
colnames(data)

datadb = dbConnect(SQLite(), dbname = 'data.sqlite')

dbWriteTable(conn = datadb, name = "app", data, overwrite=T,
             row.names=FALSE)


ssn = dbGetQuery(datadb, 
                 "SELECT a.record,
                 COUNT(CASE WHEN a.date = b.date AND a.record > b.record THEN a.record ELSE NULL END) AS same_ssn_1,
                 COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 2 AND a.record > b.record THEN a.record ELSE NULL END) AS same_ssn_3,
                 COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 6 AND a.record > b.record THEN a.record ELSE NULL END) AS same_ssn_7,
                 COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 13 AND a.record > b.record THEN a.record ELSE NULL END) AS same_ssn_14,
                 COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 29 AND a.record > b.record THEN a.record ELSE NULL END) AS same_ssn_30,
                 COUNT(DISTINCT CASE WHEN a.date = b.date AND a.record > b.record THEN b.address ELSE NULL END) AS same_ssn_diff_address_1, 
                 COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 AND a.record > b.record THEN b.address ELSE NULL END) AS same_ssn_diff_address_3,
                 COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 AND a.record > b.record THEN b.address ELSE NULL END) AS same_ssn_diff_address_7,
                 COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 AND a.record > b.record THEN b.address ELSE NULL END) AS same_ssn_diff_address_14,
                 COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 AND a.record > b.record THEN b.address ELSE NULL END) AS same_ssn_diff_address_30,
                 COUNT(DISTINCT CASE WHEN a.date = b.date AND a.record > b.record THEN b.homephone ELSE NULL END) AS same_ssn_diff_phone_1, 
                 COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 AND a.record > b.record THEN b.homephone ELSE NULL END) AS same_ssn_diff_phone_3,
                 COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 AND a.record > b.record THEN b.homephone ELSE NULL END) AS same_ssn_diff_phone_7,
                 COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 AND a.record > b.record THEN b.homephone ELSE NULL END) AS same_ssn_diff_phone_14,
                 COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 AND a.record > b.record THEN b.homephone ELSE NULL END) AS same_ssn_diff_phone_30,
                 COUNT(DISTINCT CASE WHEN a.date = b.date AND a.record > b.record THEN b.firstname || b.dob || b.lastname ELSE NULL END) AS same_ssn_diff_bdname_1, 
                 COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 AND a.record > b.record THEN b.firstname || b.dob || b.lastname ELSE NULL END) AS same_ssn_diff_bdname_3,
                 COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 AND a.record > b.record THEN b.firstname || b.dob || b.lastname ELSE NULL END) AS same_ssn_diff_bdname_7,
                 COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 AND a.record > b.record THEN b.firstname || b.dob || b.lastname ELSE NULL END) AS same_ssn_diff_bdname_14,
                 COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 AND a.record > b.record THEN b.firstname || b.dob || b.lastname ELSE NULL END) AS same_ssn_diff_bdname_30,
                 COUNT(DISTINCT CASE WHEN a.date = b.date AND a.record > b.record THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) AS same_ssn_diff_zipname_1, 
                 COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 AND a.record > b.record THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) AS same_ssn_diff_zipname_3,
                 COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 AND a.record > b.record THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) AS same_ssn_diff_zipname_7,
                 COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 AND a.record > b.record THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) AS same_ssn_diff_zipname_14,
                 COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 AND a.record > b.record THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) AS same_ssn_diff_zipname_30
                 FROM app a, app b
                 WHERE a.date - b.date BETWEEN 0 AND 29 
                 AND a.ssn = b.ssn
                 GROUP BY 1")
View(ssn)

address = dbGetQuery(datadb, 
                     "SELECT a.record,
                     COUNT(CASE WHEN a.date = b.date AND a.record > b.record THEN a.record ELSE NULL END) AS same_address_1,
                     COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 2 AND a.record > b.record THEN a.record ELSE NULL END) AS same_address_3,
                     COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 6 AND a.record > b.record THEN a.record ELSE NULL END) AS same_address_7,
                     COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 13 AND a.record > b.record THEN a.record ELSE NULL END) AS same_address_14,
                     COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 29 AND a.record > b.record THEN a.record ELSE NULL END) AS same_address_30,
                     COUNT(DISTINCT CASE WHEN a.date = b.date AND a.record > b.record THEN b.ssn ELSE NULL END) AS same_address_diff_ssn_1, 
                     COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 AND a.record > b.record THEN b.ssn ELSE NULL END) AS same_address_diff_ssn_3,
                     COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 AND a.record > b.record THEN b.ssn ELSE NULL END) AS same_address_diff_ssn_7,
                     COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 AND a.record > b.record THEN b.ssn ELSE NULL END) AS same_address_diff_ssn_14,
                     COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 AND a.record > b.record THEN b.ssn ELSE NULL END) AS same_address_diff_ssn_30,
                     COUNT(DISTINCT CASE WHEN a.date = b.date AND a.record > b.record THEN b.homephone ELSE NULL END) AS same_address_diff_phone_1, 
                     COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 AND a.record > b.record THEN b.homephone ELSE NULL END) AS same_address_diff_phone_3,
                     COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 AND a.record > b.record THEN b.homephone ELSE NULL END) AS same_address_diff_phone_7,
                     COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 AND a.record > b.record THEN b.homephone ELSE NULL END) AS same_address_diff_phone_14,
                     COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 AND a.record > b.record THEN b.homephone ELSE NULL END) AS same_address_diff_phone_30,
                     COUNT(DISTINCT CASE WHEN a.date = b.date AND a.record > b.record THEN b.firstname || b.dob || b.lastname ELSE NULL END) AS same_address_diff_bdname_1, 
                     COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 AND a.record > b.record THEN b.firstname || b.dob || b.lastname ELSE NULL END) AS same_address_diff_bdname_3,
                     COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 AND a.record > b.record THEN b.firstname || b.dob || b.lastname ELSE NULL END) AS same_address_diff_bdname_7,
                     COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 AND a.record > b.record THEN b.firstname || b.dob || b.lastname ELSE NULL END) AS same_address_diff_bdname_14,
                     COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 AND a.record > b.record THEN b.firstname || b.dob || b.lastname ELSE NULL END) AS same_address_diff_bdname_30,
                     COUNT(DISTINCT CASE WHEN a.date = b.date AND a.record > b.record THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) AS same_address_diff_zipname_1, 
                     COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 AND a.record > b.record THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) AS same_address_diff_zipname_3,
                     COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 AND a.record > b.record THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) AS same_address_diff_zipname_7,
                     COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 AND a.record > b.record THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) AS same_address_diff_zipname_14,
                     COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 AND a.record > b.record THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) AS same_address_diff_zipname_30
                     FROM app a, app b
                     WHERE a.date - b.date BETWEEN 0 AND 29 
                     AND a.address = b.address
                     GROUP BY 1")


phone = dbGetQuery(datadb, 
                   "SELECT a.record,
                   COUNT(CASE WHEN a.date = b.date AND a.record > b.record THEN a.record ELSE NULL END) AS same_phone_1,
                   COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 2 AND a.record > b.record THEN a.record ELSE NULL END) AS same_phone_3,
                   COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 6 AND a.record > b.record THEN a.record ELSE NULL END) AS same_phone_7,
                   COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 13 AND a.record > b.record THEN a.record ELSE NULL END) AS same_phone_14,
                   COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 29 AND a.record > b.record THEN a.record ELSE NULL END) AS same_phone_30,
                   COUNT(DISTINCT CASE WHEN a.date = b.date AND a.record > b.record THEN b.ssn ELSE NULL END) AS same_phone_diff_ssn_1, 
                   COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 AND a.record > b.record THEN b.ssn ELSE NULL END) AS same_phone_diff_ssn_3,
                   COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 AND a.record > b.record THEN b.ssn ELSE NULL END) AS same_phone_diff_ssn_7,
                   COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 AND a.record > b.record THEN b.ssn ELSE NULL END) AS same_phone_diff_ssn_14,
                   COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 AND a.record > b.record THEN b.ssn ELSE NULL END) AS same_phone_diff_ssn_30,
                   COUNT(DISTINCT CASE WHEN a.date = b.date AND a.record > b.record THEN b.address ELSE NULL END) AS same_phone_diff_address_1, 
                   COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 AND a.record > b.record THEN b.address ELSE NULL END) AS same_phone_diff_address_3,
                   COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 AND a.record > b.record THEN b.address ELSE NULL END) AS same_phone_diff_address_7,
                   COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 AND a.record > b.record THEN b.address ELSE NULL END) AS same_phone_diff_address_14,
                   COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 AND a.record > b.record THEN b.address ELSE NULL END) AS same_phone_diff_address_30,
                   COUNT(DISTINCT CASE WHEN a.date = b.date AND a.record > b.record THEN b.firstname || b.dob || b.lastname ELSE NULL END) AS same_phone_diff_bdname_1, 
                   COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 AND a.record > b.record THEN b.firstname || b.dob || b.lastname ELSE NULL END) AS same_phone_diff_bdname_3,
                   COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 AND a.record > b.record THEN b.firstname || b.dob || b.lastname ELSE NULL END) AS same_phone_diff_bdname_7,
                   COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 AND a.record > b.record THEN b.firstname || b.dob || b.lastname ELSE NULL END) AS same_phone_diff_bdname_14,
                   COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 AND a.record > b.record THEN b.firstname || b.dob || b.lastname ELSE NULL END) AS same_phone_diff_bdname_30,
                   COUNT(DISTINCT CASE WHEN a.date = b.date AND a.record > b.record THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) AS same_phone_diff_zipname_1, 
                   COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 AND a.record > b.record THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) AS same_phone_diff_zipname_3,
                   COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 AND a.record > b.record THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) AS same_phone_diff_zipname_7,
                   COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 AND a.record > b.record THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) AS same_phone_diff_zipname_14,
                   COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 AND a.record > b.record THEN b.firstname || b.zip5 || b.lastname ELSE NULL END) AS same_phone_diff_zipname_30
                   FROM app a, app b
                   WHERE a.date - b.date BETWEEN 0 AND 29 
                   AND a.homephone = b.homephone
                   GROUP BY 1")


dbname = dbGetQuery(datadb, 
                    "SELECT a.record,
                    COUNT(CASE WHEN a.date = b.date AND a.record > b.record THEN a.record ELSE NULL END) AS same_dbname_1,
                    COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 2 AND a.record > b.record THEN a.record ELSE NULL END) AS same_dbname_3,
                    COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 6 AND a.record > b.record THEN a.record ELSE NULL END) AS same_dbname_7,
                    COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 13 AND a.record > b.record THEN a.record ELSE NULL END) AS same_dbname_14,
                    COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 29 AND a.record > b.record THEN a.record ELSE NULL END) AS same_dbname_30,
                    COUNT(DISTINCT CASE WHEN a.date = b.date AND a.record > b.record THEN b.ssn ELSE NULL END) AS same_dbname_diff_ssn_1, 
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 AND a.record > b.record THEN b.ssn ELSE NULL END) AS same_dbname_diff_ssn_3,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 AND a.record > b.record THEN b.ssn ELSE NULL END) AS same_dbname_diff_ssn_7,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 AND a.record > b.record THEN b.ssn ELSE NULL END) AS same_dbname_diff_ssn_14,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 AND a.record > b.record THEN b.ssn ELSE NULL END) AS same_dbname_diff_ssn_30,
                    COUNT(DISTINCT CASE WHEN a.date = b.date AND a.record > b.record THEN b.address ELSE NULL END) AS same_dbname_diff_address_1, 
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 AND a.record > b.record THEN b.address ELSE NULL END) AS same_dbname_diff_address_3,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 AND a.record > b.record THEN b.address ELSE NULL END) AS same_dbname_diff_address_7,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 AND a.record > b.record THEN b.address ELSE NULL END) AS same_dbname_diff_address_14,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 AND a.record > b.record THEN b.address ELSE NULL END) AS same_dbname_diff_address_30,
                    COUNT(DISTINCT CASE WHEN a.date = b.date AND a.record > b.record THEN b.homephone ELSE NULL END) AS same_dbname_diff_phone_1, 
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 AND a.record > b.record THEN b.homephone ELSE NULL END) AS same_dbname_diff_phone_3,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 AND a.record > b.record THEN b.homephone ELSE NULL END) AS same_dbname_diff_phone_7,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 AND a.record > b.record THEN b.homephone ELSE NULL END) AS same_dbname_diff_phone_14,
                    COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 AND a.record > b.record THEN b.homephone ELSE NULL END) AS same_dbname_diff_phone_30
                    FROM app a, app b
                    WHERE a.date - b.date BETWEEN 0 AND 29 
                    AND a.firstname = b.firstname
                    AND a.lastname = b.lastname
                    AND a.dob = b.dob
                    GROUP BY 1")


zipname = dbGetQuery(datadb, 
                     "SELECT a.record,
                     COUNT(CASE WHEN a.date = b.date AND a.record > b.record THEN a.record ELSE NULL END) AS same_zipname_1,
                     COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 2 AND a.record > b.record THEN a.record ELSE NULL END) AS same_zipname_3,
                     COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 6 AND a.record > b.record THEN a.record ELSE NULL END) AS same_zipname_7,
                     COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 13 AND a.record > b.record THEN a.record ELSE NULL END) AS same_zipname_14,
                     COUNT(CASE WHEN a.date - b.date BETWEEN 0 AND 29 AND a.record > b.record THEN a.record ELSE NULL END) AS same_zipname_30,
                     COUNT(DISTINCT CASE WHEN a.date = b.date AND a.record > b.record THEN b.ssn ELSE NULL END) AS same_zipname_diff_ssn_1, 
                     COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 AND a.record > b.record THEN b.ssn ELSE NULL END) AS same_zipname_diff_ssn_3,
                     COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 AND a.record > b.record THEN b.ssn ELSE NULL END) AS same_zipname_diff_ssn_7,
                     COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 AND a.record > b.record THEN b.ssn ELSE NULL END) AS same_zipname_diff_ssn_14,
                     COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 AND a.record > b.record THEN b.ssn ELSE NULL END) AS same_zipname_diff_ssn_30,
                     COUNT(DISTINCT CASE WHEN a.date = b.date AND a.record > b.record THEN b.address ELSE NULL END) AS same_zipname_diff_address_1, 
                     COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 AND a.record > b.record THEN b.address ELSE NULL END) AS same_zipname_diff_address_3,
                     COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 AND a.record > b.record THEN b.address ELSE NULL END) AS same_zipname_diff_address_7,
                     COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 AND a.record > b.record THEN b.address ELSE NULL END) AS same_zipname_diff_address_14,
                     COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 AND a.record > b.record THEN b.address ELSE NULL END) AS same_zipname_diff_address_30,
                     COUNT(DISTINCT CASE WHEN a.date = b.date AND a.record > b.record THEN b.homephone ELSE NULL END)  AS same_zipname_diff_phone_1, 
                     COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 2 AND a.record > b.record THEN b.homephone ELSE NULL END) AS same_zipname_diff_phone_3,
                     COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 6 AND a.record > b.record THEN b.homephone ELSE NULL END) AS same_zipname_diff_phone_7,
                     COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 13 AND a.record > b.record THEN b.homephone ELSE NULL END) AS same_zipname_diff_phone_14,
                     COUNT(DISTINCT CASE WHEN a.date - b.date BETWEEN 0 AND 29 AND a.record > b.record THEN b.homephone ELSE NULL END) AS same_zipname_diff_phone_30
                     FROM app a, app b
                     WHERE a.date - b.date BETWEEN 0 AND 29 
                     AND a.firstname = b.firstname
                     AND a.lastname = b.lastname
                     AND a.zip5 = b.zip5
                     GROUP BY 1")

data1 = merge(data, address)
data1 = merge(data1, ssn)
data1 = merge(data1, phone)
data1 = merge(data1, dbname)
data1 = merge(data1, zipname) 

View(data1)

for(i in 61:85) {
  data1[data1$homephone == 9105580920, i] = 0
}


for(i in 36:60) {
  data1[data1$ssn == 737610282, i] = 0
} 


write.csv(data1, "data.csv")


##Checks

library(dplyr)

View(data1)

##number of distinct values in each column
data1 %>%
  summarise_each(funs(n_distinct))





