import pymongo
import psycopg2

#postgresql
postgresqlConnection = psycopg2.connect(user="doadmin", password="AVNS_oxW-lUXpsW7VRy-mtuy", database="defaultdb", host="db-postgresql-dbit-apps-do-user-2230473-0.b.db.ondigitalocean.com", port="25060")
sqlCursor = postgresqlConnection.cursor()

#mongdb
client = pymongo.MongoClient('mongodb+srv://jgme4350:jgme4350@cluster0.nkehi.mongodb.net/dbit_control_db?retryWrites=true&w=majority')
db = client.dbit_control_db
collection = db.dbitapps
cursor = collection.find({})
allApps = []
for document in cursor:
    urlid = document['urlid']
    docid = document['id']
    appName = document['name']
    arr = appName.split('-')
    newAppName = (arr[0].lower())+"_"+arr[1]
    if(newAppName[0]=='0' or newAppName[0]=='1' or newAppName[0]=='2' or newAppName[0]=='3' or newAppName[0]=='4' or newAppName[0]=='5' or newAppName[0]=='6' or newAppName[0]=='7' or newAppName[0]=='8' or newAppName[0]=='9'):
      newAppName = "_"+newAppName
    docServer = document['server']
    dataSize = document['datasize']
    appCounted = newAppName in allApps
    if(appCounted):
      continue
    else:
      allApps.append(newAppName)
    sqlCursor.execute("INSERT INTO app_primary_table (id, app_name, app_server, app_logger, app_summery_id, app_user_list_id,user_list_size) VALUES(%s, %s, %s, %s, %s, %s, %s)", (docid, newAppName, docServer, urlid, newAppName+"_summery", newAppName+"_list_id", 0))
    print(newAppName)



postgresqlConnection.commit()
client.close()
sqlCursor.close()
postgresqlConnection.close()

