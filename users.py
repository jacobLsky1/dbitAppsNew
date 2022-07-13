import null as null
import psycopg2
from psycopg2 import sql
from bs4 import BeautifulSoup
from collections import defaultdict
from datetime import datetime
import requests
import json

# postgresql
postgresqlConnection = psycopg2.connect(user="dibit", password="jgme4350jldbit", database="dbit_apps", host="localhost",
                                        port="5432")
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

    if appLogger != "null":

        name = appName + "_list_id"
        columns = (("date", "VARCHAR(255)"), ("time", "VARCHAR(255)"), ("valid_status", "VARCHAR(255)"),
                   ("device_type", "VARCHAR(255)"),
                   ("country_code", "VARCHAR(255)"), ("region_name", "VARCHAR(255)"), ("ip_address", "VARCHAR(255)"),
                   ("user_agent", "VARCHAR(255)"))
        fields = []
        for col in columns:
            fields.append(sql.SQL("{} {}").format(sql.Identifier(col[0]), sql.SQL(col[1])))

        query = sql.SQL("CREATE TABLE IF NOT EXISTS {tbl_name} ( {fields} );").format(
            tbl_name=sql.Identifier(name),
            fields=sql.SQL(', ').join(fields)
        )
        sqlCursor.execute(query)
        print(f"created table {appName + '_list_id'}")
        urlPage = requests.get(appLogger)
        soup = BeautifulSoup(urlPage.text, 'html.parser')
        myString = str(soup)
        myList = myString.split('%')
        myList.pop(0)
        jsonList = []
        num = 0
        for item in myList:
            json_object = json.loads(item)
            try:
                datetime = json_object['datetime']
            except:
                datetime = json_object['date_time']

            try:
                status = json_object['status']
            except:
                status = json_object['user_status']

            try:
                devicetype = json_object['devicetype']
            except:
                devicetype = json_object['device_type']

            country_code = json_object['country_code']
            region_name = json_object['region_name']

            try:
                myip = json_object['myip']
            except:
                myip = json_object['user_ip']

            useragent = json_object['useragent']

            if datetime is not None and myip is not None and status is not None and useragent is not None and devicetype is not None and country_code is not None and region_name is not None:
                if len(useragent) > 255:
                    useragent = useragent[:254]

                datetimeTemp = datetime.split(' ')
                date = datetimeTemp[0]
                time = datetimeTemp[1]

                sqlCursor.execute(sql.SQL(
                    "INSERT INTO {tbl_name} (date, time, valid_status, device_type, country_code, region_name, ip_address, user_agent) values (%s, %s, %s,%s, %s,%s, %s, %s)"
                ).format(tbl_name=sql.Identifier(name)),
                                  [date, time, status, devicetype, country_code, region_name, myip, useragent])
                num = num + 1
                print(f"{name} - {num} out of {len(myList)}")

        print(f"added all {num} data items to {appName + '_list_id'}")
        newquery = sql.SQL("UPDATE app_primary_table SET user_list_size = %s WHERE app_name = %s")
        sqlCursor.execute(newquery, [num, appName])
        print(f"set {appName} user size to {num}")
        print("")
        print("")
        print("")
        print("")

postgresqlConnection.commit()
sqlCursor.close()
postgresqlConnection.close()
