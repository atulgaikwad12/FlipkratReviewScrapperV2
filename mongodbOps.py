import pymongo, configHandler as cfg

class mongodbOps():

    def __init__(self, server,clg):
        self.clg = clg
        self.server = server
        self.isConnected = self.connectMongo()


    def __str__(self):
        return self.server + str(self.client)

    @staticmethod #As this method is not using any instance or class attribute and also now we can call it directly using class name without object
    def isValidDict(dictData):
        if type(dictData) == dict:
            return True
        else:
            raise Exception('Invalid JSON schema')

    @classmethod #Using this function to return object of class and also now we can call it directly using class name without object
    def getConnObject(cls,clg):
        # here will read config file and as per that will pass required data to create final db connection instance
        ch = cfg.configHandler("dbconnections.ini")
        res = ch.readConfigFile("mongodb")
        if(type(res) == tuple):
            server = res[0]
        else:
            server = res

        return cls(server,clg)

    def connectMongo(self):
        '''
        :return: It will return True if db connection is successful else will return False
        '''

        try:
            self.client = pymongo.MongoClient(self.server)
            msg = "Connected mongo db with server **** {} , client object {}".format(self.server, self.client)
            self.clg.log(msg)
        except Exception as e:
            msg = "Couldn't connect mongo db server - {} ".format(self.server)
            msg = msg + "\nGetting error " + str(e)
            self.clg.log(msg, "ERROR")
            return False
        else:
            return True

    def insertData(self,dbname, collectionName,insRecord):
        '''
        :param collectionName: provide valid collection name
        :param insRecord: document to insert in JSON format
        :return: Will return True on successful insertion
        '''
        if(self.isConnected):
            try:
                dBConn = self.client[dbname]
                dbCollect = dBConn[collectionName]
                msg = 'Using db - {} collection - {} '.format(dbname, collectionName)
                msg = msg + '\nReceived for insertion - ' + str(insRecord)

                if(type(insRecord) == list): # for multiple records
                    for i in insRecord:
                        mongodbOps.isValidDict(i)
                    dbCollect.insert_many(insRecord)
                else:
                    mongodbOps.isValidDict(insRecord)
                    dbCollect.insert_one(insRecord)

                msg = msg + '\nData Inserted successfully'
                self.clg.log(msg)
                return {"OUTPUT" : "Data Inserted successfully"}
            except Exception as e:
                msg = 'Something went wrong couldn\'t fetch data getting error - ' + str(e)
                self.clg.log(msg, "ERROR")
                return {"ERROR" : "Something went wrong Please check your input"}
        else:
            return {"ERROR": "Database not connected"}

    def readData(self, dbname,collectionName, findJSON=None, sortColumn=''):
        '''
        :param collectionName: provide valid collection name
        :param findJSON: (optional)  select criteria for find query in JSON format
        :param sortColumn: (optional) column name to sort data based on
        :return: Will return result in dictionary data type
        '''
        if(self.isConnected):
            try:
                dBConn = self.client[dbname]
                dbCollect = dBConn[collectionName]
                msg = 'Using db - {} collection - {} '.format(dbname, collectionName)
                msg = msg + '\nReceived find query -{} sort column {} '.format(findJSON,sortColumn)
                mongodbOps.isValidDict(findJSON)

                if (findJSON is not None and sortColumn != ''):
                    cursor = dbCollect.find(findJSON).sort(sortColumn)
                elif (findJSON is not None and sortColumn == ''):
                    cursor = dbCollect.find(findJSON)
                else:
                    cursor = dbCollect.find()

                result = []
                for row in cursor:
                    if(row['_id'] is not None):
                        row['_id'] = str(row['_id'])
                    result.append(row)

                msg = msg + '\nData Fetched successfully'
                self.clg.log(msg)
                return result
            except Exception as e:
                msg = 'Something went wrong couldn\'t fetch data getting error - ' + str(e)
                self.clg.log(msg, "ERROR")
                return {"ERROR" : "Something went wrong Please check your input"}
        else:
            return {"ERROR": "Database not connected"}

    def updateData(self, dbname,collectionName, whereJSON, updJSON, updFlag=2):
        '''

        :param collectionName: provide valid collection name
        :param whereJSON: where condition for update query in JSON format
        example - {"Personal_details.sex":"M"}
        :param updJSON:  update data of mongo update query in JSON format
        example - {"$set": {"Personal_details.sex":"Male"},
                                "$currentDate" : {"last_modified" : True}}
        :param updFlag: (optional) pass 1  for update one,
        pass 2  for update many (this is by default),
        pass -1  for replace one
        :return: Will return True if updated successfully else False
        '''
        if (self.isConnected):
            try:
                dBConn = self.client[dbname]
                dbCollect = dBConn[collectionName]
                msg = 'Using db - {} collection - {} '.format(dbname, collectionName)

                mongodbOps.isValidDict(whereJSON)
                mongodbOps.isValidDict(updJSON)
                msg = msg + '\nwhereJSON --->' + str(whereJSON)
                msg = msg + '\nupdJSON --->' + str(updJSON)
                msg = msg + '\nupdFlag --->' + str(updFlag)

                if (updFlag == 1):
                    dbCollect.update_one(whereJSON, updJSON)
                elif (updFlag == -1):
                    dbCollect.replace_one(whereJSON, updJSON)
                else:
                    dbCollect.update_many(whereJSON, updJSON)

            except Exception as e:
                msg = 'Something went wrong couldn\'t update data getting error - ' + str(e)
                self.clg.log(msg, "ERROR")
                return {"ERROR" : "Something went wrong Please check your input"}
            else:
                msg = msg + '\nData updated successfully'
                self.clg.log(msg)
                return {"OUTPUT" : "Data Updated successfully"}
        else:
            return {"ERROR": "Database not connected"}

    def deleteData(self, dbname,collectionName, whereJSON, dltFlag=2):
        '''
        :param collectionName: provide valid collection name
        :param whereJSON: where condition for delete query in JSON format
        :param dltFlag:(optional) pass 1 to delete single record
        else pass any number greater than 1 for multiple record deletion (this is by default )
        :return:
        '''
        if (self.isConnected):
            try:
                dBConn = self.client[dbname]
                dbCollect = dBConn[collectionName]
                msg = 'Using db - {} collection - {} '.format(dbname, collectionName)

                mongodbOps.isValidDict(whereJSON)
                mongodbOps.isValidDict(updJSON)
                msg = msg + '\nwhereJSON --->' + str(whereJSON)
                msg = msg + '\ndltFlag --->' + str(updJSON)

                if (updFlag == 1):
                    dbCollect.delete_one(whereJSON)
                else:
                    dbCollect.delete_many(whereJSON)

            except Exception as e:
                msg = 'Something went wrong couldn\'t delete data getting error - ' + str(e)
                self.clg.log(msg,"ERROR")
                return {"ERROR" : "Something went wrong Please check your input"}
            else:
                msg = msg + '\nData deleted successfully'
                self.clg.log(msg)
                return {"OUTPUT" : "Data Deleted successfully"}
        else:
            return {"ERROR": "Database not connected"}

if(__name__ == "__main__"):
    import customLogger as lgr
    clg = lgr.customLogger(__name__)
    mdbConn = mongodbOps.getConnObject(clg)
    findjson = {"$and": [{"lastname": {"$in": ["Gaikwad", "Dalavi"]}}, {"Personal_details.age": {"$gt": 21}}]}
    output = mdbConn.readData('Employee', 'employeeInformation', findjson, 'Joining_date')
    #output = mdbConn.readData('Employee', 'employeeInformation')
    print('Response- {}'.format(output))
