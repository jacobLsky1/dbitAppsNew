import psycopg2
from psycopg2 import sql
from bs4 import BeautifulSoup
from collections import defaultdict
from datetime import datetime
import requests
import json



# postgresql
postgresqlConnection = psycopg2.connect(user="dibit", password="AVNS_nmcfJN5ksfH5HPub9ph", database="dbit_apps", host="db-postgresql-dbit-apps-do-user-2230473-0.b.db.ondigitalocean.com",
                                        port="25060")
sqlCursor = postgresqlConnection.cursor()
postgreSQL_select_Query = "SELECT * FROM app_primary_table"
sqlCursor.execute(postgreSQL_select_Query)
all_apps_records = sqlCursor.fetchall()
for row in all_apps_records:
    appId = row[0]
    appName = row[1]
    appServer = row[2]
    appLogger = row[3]
    appSummeryId = row[4]
    appListId = row[5]
    appListSize = row[6]


    if appLogger != "null":

        name = appName + "_summery"
        columns = (("date", "VARCHAR(50)"), ("amount_of_entries", "BIGINT"), ("entries_by_country", "TEXT[]"),
                   ("invalid_with_data", "TEXT[]"), ("mobile_entries_amount", "BIGINT"), ("desktop_entries_amount", "BIGINT"))
        fields = []
        for col in columns:
            fields.append(sql.SQL("{} {}").format(sql.Identifier(col[0]), sql.SQL(col[1])))

        query = sql.SQL("CREATE TABLE IF NOT EXISTS {tbl_name} ( {fields} );").format(
            tbl_name=sql.Identifier(name),
            fields=sql.SQL(', ').join(fields)
        )
        sqlCursor.execute(query)

        # getting distinct dates from user table
        postgres_select_date_from_users_Query = sql.SQL("SELECT DISTINCT date FROM {tbl_name} ORDER BY date;").format(
            tbl_name=sql.Identifier(appListId)
        )
        sqlCursor.execute(postgres_select_date_from_users_Query)
        dateListFromUsers = sqlCursor.fetchall()

        # getting distinct dates from summery table
        postgres_select_date_from_summery_Query = sql.SQL("SELECT DISTINCT date FROM {tbl_name} ORDER BY date;").format(
            tbl_name=sql.Identifier(appSummeryId)
        )
        sqlCursor.execute(postgres_select_date_from_summery_Query)
        dateListFromSummery = sqlCursor.fetchall()

        if dateListFromSummery:
          lastdate = dateListFromSummery[len(dateListFromSummery)-1]
          postgres_delete_last_date_from_summery_Query = sql.SQL("DELETE FROM {tbl_name} WHERE date = %s;").format(
              tbl_name=sql.Identifier(appSummeryId)
          )
          sqlCursor.execute(postgres_delete_last_date_from_summery_Query,[lastdate[0]])
          dateListFromSummery.pop()

        dateList = []
   
        datesSummery = []

        for date in dateListFromSummery:
            datesSummery.append(date[0])

        for date in dateListFromUsers:
            if date[0] not in datesSummery:
                dateList.append(date)


        #for every date in table
        for date in dateList:

            dateStr = date[0]
            amount_of_entries= 0
            entries_by_country = []
            invalid_with_data = []
            mobile_entries_amount = 0
            desktop_entries_amount = 0

            #gets users by date
            postgres_select_by_date_Query = sql.SQL("SELECT * FROM {tbl_name} WHERE date = %s;").format(
            tbl_name=sql.Identifier(appListId)
            )
            sqlCursor.execute(postgres_select_by_date_Query,[dateStr])
            entriesList = sqlCursor.fetchall()
            amount_of_entries = len(entriesList)

            #gets the distinct country code in the table
            postgres_select_by_country_Query = sql.SQL("SELECT DISTINCT country_code FROM {tbl_name} WHERE date = %s;").format(
            tbl_name=sql.Identifier(appListId)
            )
            sqlCursor.execute(postgres_select_by_country_Query,[dateStr])
            countryList = sqlCursor.fetchall()

            #sorting users by country
            for countryCode in countryList:
                #gets the users by country code and date in the table
                countryCodeStr = countryCode[0]
                postgres_select_users_by_country_Query = sql.SQL("SELECT * FROM {tbl_name} WHERE date = %s AND country_code = %s;").format(
                tbl_name=sql.Identifier(appListId)
                )
                sqlCursor.execute(postgres_select_users_by_country_Query,[dateStr,countryCodeStr])
                usersByCodeList = sqlCursor.fetchall()
                amount_of_users_by_code = len(usersByCodeList)
                entries_by_country.append(f"{countryCodeStr} {amount_of_users_by_code}")
            
            # getting invalid Data
            postgres_select_inValid_users_Query = sql.SQL("SELECT * FROM {tbl_name} WHERE date = %s AND valid_status != %s;").format(
            tbl_name=sql.Identifier(appListId)
            )
            sqlCursor.execute(postgres_select_inValid_users_Query,[dateStr,"Valid"])
            inValidUsersList = sqlCursor.fetchall()
            #sorting invalid users into data
            for user in inValidUsersList:
                userDate = user[0]
                userTime = user[1]
                userValid = user[2]
                userDevice = user[3]
                userCode = user[4]
                userArea = user[5]
                userIP = user[6]
                userAgent = user[7]
                dataStr = ""+userTime +"|"+ userValid +"|"+ userDevice +"|"+ userCode +"|"+ userArea +"|"+ userIP+"|"+userAgent
                invalid_with_data.append(dataStr)
            
            #getting Mobile Amount
            postgres_select_mobile_users_Query = sql.SQL("SELECT * FROM {tbl_name} WHERE date = %s AND device_type = %s;").format(
            tbl_name=sql.Identifier(appListId)
            )
            sqlCursor.execute(postgres_select_mobile_users_Query,[dateStr,"Mobile"])
            mobileUsersList = sqlCursor.fetchall()
            mobile_entries_amount = len(mobileUsersList)

            #getting Desktop Amount
            postgres_select_desktop_users_Query = sql.SQL("SELECT * FROM {tbl_name} WHERE date = %s AND device_type != %s;").format(
            tbl_name=sql.Identifier(appListId)
            )
            sqlCursor.execute(postgres_select_desktop_users_Query,[dateStr,"Mobile"])
            mobileUsersList = sqlCursor.fetchall()
            desktop_entries_amount = len(mobileUsersList)

            #inserting all into database
            sqlCursor.execute(sql.SQL(
                "INSERT INTO {tbl_name} (date, amount_of_entries, entries_by_country, invalid_with_data, mobile_entries_amount, desktop_entries_amount) values (%s, %s, %s,%s, %s,%s)"
            ).format(tbl_name=sql.Identifier(name)),[dateStr, amount_of_entries, entries_by_country, invalid_with_data, mobile_entries_amount, desktop_entries_amount])
            print(f"{name} - added data for date {dateStr} ")
            





postgresqlConnection.commit()
sqlCursor.close()
postgresqlConnection.close()
