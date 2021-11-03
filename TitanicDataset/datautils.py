import pandas as pd
from configparser import ConfigParser
import mysql
from mysql.connector import Error

class DataExtractor(object):
    """
        Class extracts the data from the database, whose information is set in mypy.ini file
    """
    __data__ = None
    __sql_server_info__ = None
    __user_info__ = None
    conn = None
    cur = None

    def __init__(self) -> None:
        co = ConfigParser()
        co.read("config.ini")
        self.__sql_server_info__ = co["SQLServer"]
        self.__user_info__ = co["UserInfo"]
        print("Connecting to Database....")
        self._db_init_()

    def set_data(self, data: pd.DataFrame) -> None:
        self.__data__ = data

    def get_data(self) -> pd.DataFrame:
        return self.__data__

    def _db_init_(self) -> None:
        try:
            self.conn = mysql.connector.connect(host=self.__sql_server_info__["host"], database=self.__sql_server_info__["database"],
                                           user=self.__user_info__["user"], password=self.__user_info__["password"])
            if self.conn.is_connected():
                self.cur = self.conn.cursor()
                self.cur.execute("SELECT DATABASE()")
                db = self.cur.fetchone()
                print("Connected to: {0} Database".format(db[0]))
        except Error as e:
            print(e)

    def pull_data_from_db(self, *, table_name: str) -> pd.DataFrame:
        dd = dict()
        sql = """
            SELECT * FROM {0};
        """.format(table_name)
        print("Pulling data from {0} table".format(table_name))
        self.cur.execute(sql)
        data = self.cur.fetchall()
        sql = """
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = \'{0}\'
        """.format(table_name)
        self.cur.execute(sql)
        col = self.cur.fetchall()
        col = [col[i][0] for i in range(0, len(col))]
        dd['data'] = data
        dd['columns'] = col
        res_df = pd.DataFrame(data=dd['data'], columns=dd['columns'])
        print("Pulling completed")
        self.set_data(data=res_df)
        return res_df
    
class FeatureEngineering(DataExtractor):
    def __init__(self):
        super().__init__()

if __name__ == "__main__":
    obj = FeatureEngineering()
    boj.get_data()