from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import configHandler as cfg
import pandas as pd

class cassandraOps:

    def __init__(self,clg):
        self.clg = clg
        self.isConnected = self.connectDataStax()

    def connectDataStax(self):
        '''
        :return: It will return session if db connection is successful else will return False
        '''
        try:
            ch      = cfg.configHandler("config.ini")
            options = ch.readConfigSection("cassandra")

            bundle          = options['bundle']
            client_id       = options['client_id']
            client_secret   = options['client_secret']
            key_space       = options['key_space']

            cloud_config = {
                'secure_connect_bundle': bundle
            }
            auth_provider = PlainTextAuthProvider(client_id,client_secret)
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            self.session = cluster.connect(key_space)
            self.useKeySpace(self.session,key_space)

            msg = "Connected cassandra db with cluster - {} and keyspace - {}".format(str(cluster),key_space)
            self.clg.log(msg)

        except Exception as e:
            msg = "Couldn't connect cassandra db server Getting error " + str(e)
            self.clg.log(msg,"ERROR")
            return False
        else:
            return True

    def useKeySpace(self,session,keyspace):
        """
        Function will create keyspace if not present
        :param session: session required to connect cassandra
        :param keyspace: keyspace to search
        :return:
        """
        try:
            searchQry = "SELECT * FROM system_schema.keyspaces WHERE keyspace_name=" + keyspace
            result = session.execute(searchQry).one()

            if (result):
                msg = " keyspace " + keyspace + "exist"
            else:
                msg = "creating new keyspace"
                createQry = "CREATE KEYSPACE "+keyspace+" WITH replication={'class':'SimpleStrategy','replication_factor':'1'} AND durable_writes='true';"
                session.execute(createQry).one()

            msg = msg + "using keyspace " + keyspace
            session.execute("USE " + keyspace).one()
            self.clg.log(msg)
        except Exception as e:
            raise Exception(f"Couldn't use keyspace\n" + str(e))


    def getDataFrameOfCollection(self, table_name):
        """
        Function will accept table name and return data frame
        :param table_name: db table name
        :return: data frame of all records present under table
        """
        try:
            query = "SELECT * FROM "+ table_name
            all_Records = self.session.execute(query)
            dataframe = pd.DataFrame(all_Records)

            msg = "Fetched data from table {}".format(table_name)
            self.clg.log(msg)

            return dataframe
        except Exception as e:
            msg = "Couldn't fetch data from provided table " + str(e)
            self.clg.log(msg,"ERROR")
            return -1

    def saveDataFrameIntoCollection(self, table_name, dataframe):
        """
        function will save data frame data into table
        :param table_name: db table name
        :param dataframe: data frame
        :return: Will return True if data saved successfully else will return False
        """
        try:

            query = "INSERT INTO data(date,time,open,high,low,last) VALUES (?,?,?,?,?,?)"
            prepared = self.session.prepare(query)
            for item in dataframe:
                self.session.execute(prepared, (
                item.date_value, item.time_value, item.open_value, item.high_value, item.low_value, item.last_value))

            msg = "Saved dataframe data to provided table {}".format(table_name)
            self.clg.log(msg)
            return True
        except Exception as e:
            msg = "Couldn't save dataframe data to provided table " + str(e)
            self.clg.log(msg, "ERROR")
            return False

    def getResultToDisplayOnBrowser(self, table_name):
        """
        This function returns the final result to display on browser.
        """
        try:
            query = "select * from " + table_name
            all_Records = self.session.execute(query)
            if(len(all_Records.column_names) > 0 ):
                msg = "Fetched data from table {} for display ".format(table_name)
                self.clg.log(msg)
                return [i for i in all_Records]
        except Exception as e:
            msg = "Couldn't fetch data from provided table for diaplay " + str(e)
            self.clg.log(msg, "ERROR")
            return -1


if(__name__ == "__main__"):
    import customLogger as lgr
    clg = lgr.customLogger(__name__)
    db = cassandraOps(clg)
    # df = db.getDataFrameOfCollection("emp")
    # print(df)
    result = db.getResultToDisplayOnBrowser("emp")
    print(result)