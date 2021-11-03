import pandas as pd
from configparser import ConfigParser
import mysql
from mysql.connector import Error, connect

class DataOperator:
    """
        Class extracts the data from the database, whose information is set in mypy.ini file
    """

    # Private variables of the class DataOperator
    __data__ = None
    __data_flag__ = bool
    __sql_server_info__ = None
    __user_info__ = None
    __data_source__ = None
    __csv_file__ = None

    # Public variables of the class DataOperator
    conn = None
    cur = None

    def __init__(self) -> None:
        co = ConfigParser()
        co.read("config.ini")
        self.__sql_server_info__ = co["SQLServer"]
        self.__user_info__ = co["UserInfo"]
        self.__data_source__ = co["DataSource"]
        self.__csv_file__ = co["CSVFilePath"]
        print("Connecting to Database")
        self._data_init_()

    # Initializing the data 
    def _data_init_(self) -> None:
        try:
            if self.__data_source__["database"] == "True" and self.__data_source__["csv"] == "True":
                print("Choose only one data source in config.ini file")
                self.set_data_flag(False)
                return
            if self.__data_source__["database"] == "True":
                try:
                    if (self.__sql_server_info__["host"] is not None) and (self.__sql_server_info__["database"] is not None) \
                    and (self.__user_info__["user"] is not None) and (self.__user_info__["password"] is not None):
                        self.conn = connect(host=self.__sql_server_info__["host"], database=self.__sql_server_info__["database"], user=self.__user_info__["user"], password=self.__user_info__["password"])
                        if self.conn.is_connected():
                            self.cur = self.conn.cursor()
                            self.cur.execute("SELECT DATABASE()")
                            db = self.cur.fetchone()
                            print("Connected to: {0} Database".format(db[0]))
                            self.set_data_flag(True)
                            return 
                    else:
                        print("Value missing in the config.ini file for Database")
                        self.set_data_flag(False)
                        return 
                except Error as e:
                    print(e)
            elif self.__data_source__["csv"] == "True":
                try:
                    if self.__csv_file__["path"] is not None:
                        self.__data__ = pd.read_csv(self.__csv_file__["path"])
                        self.set_data_flag(True)
                        return 
                    else:
                        print("Path missing for the csv file in config.ini file")
                        self.set_data_flag(False)
                        return
                except Exception as e:
                    print(e)
            else:
                print("Choose the data source in the config.ini file")
        except Exception as e:
            print(e)

    # Pulling the data from the database
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
    
    def push_data_to_db(self, data: pd.DataFrame, table_name: str) -> None:
        try:
            create_table = """
                CREATE TABLE IF NOT EXISTS {0}(Attribute TEXT, Percentage float);
            """.format(table_name)
            self.cur.execute(create_table)
            print("Table created")
            for _, row in data.iterrows():
                sql = """
                    INSERT INTO {0}.{1} VALUES(%s, %s)
                """.format(self.__sql_server_info__["database"], table_name)
                self.cur.execute(sql, tuple(row))
            self.conn.commit()
        except Error as e:
            print("Error while connecting to MySQL", e)

    # Getters and Setters of the private variables
    def set_data(self, data: pd.DataFrame) -> None: self.__data__ = data
    def get_data(self) -> pd.DataFrame: return self.__data__
    def set_data_flag(self, val: bool) -> None: self.__data_flag__ = val
    def get_data_flag(self) -> bool: return self.__data_flag__


class DataCleaner(DataOperator):
    # Private variable of the class DataCleaner
    __dc_data__ = None
    __missing_threshold__ = None
    __default_col__ = None

    def __init__(self, columns: None):
        super().__init__()
        self.set_default_col(columns)
    
    # Method to impute the missing values
    def impute(self, method: "median, mode", col: list) -> None:
        """
            Imputing the missing data with the mean, median, mode values of that column
            
            ## Parameters

            method: str, with a comma(,) seperation for cont and cat variable
                method to fill the missing data (e.g: "mean, mode", where mean is used to treat 
                the missing values in continuous data and mode is used to treat the missing values 
                in categorical data).
            col: list()
                list of columns which contains the missing values and has to be filled with the 
                methods mentioned in the method variable.
            
            ## Returns

            None
        """
        if col is not None:
            method = method.split(",")
            for column in col:
                if self.__dc_data__[column].dtype == "object":
                    if method[1] == "mode":
                        self.__dc_data.fillna(value=self.__dc_data[column].mode, inplace=True)
                else:
                    if method[0] == "mean":
                        self.__dc_data[column].fillna(value=self.__dc_data[column].mean(), inplace=True)
                    if method[0] == "median":
                        self.__dc_data__[column].fillna(value=self.__dc_data__[column].median(), inplace=True)
            return
        else:
            print("Expected at least 1 column, got None")
            return

    # Method to get the missing data percentage from the dataset
    def get_missing_data_percent(self) -> pd.DataFrame:
        """
            Calculates the percentage of the missing data in the dataset and pushes the same 
            information to the database
            
            ## Parameters

            None
            
            ## Returns

            pd.DataFrame
                The DataFrame that contains the percentage of missing data in each attribute
        """
        missing_data = round(self.__dc_data__.isna().sum()/self.__dc_data.shape[0] * 100, 2)
        missing_df = pd.DataFrame(data=missing_data.reset_index())
        missing_df.columns = ["Attribute", "Percentage"]
        super().push_data_to_db(data=missing_df)
        return missing_df

    # Getters and setters for the private variables of the class DataCleaner
    def set_default_col(self, col: None) ->  None: self.__default_col__ = col
    def get_default_col(self) -> list: return self.__default_col__
    def set_missing_threshold(self, val: int) -> None: self.__missing_threshold__ = val
    def get_missing_threshold(self) -> int: return self.__missing_threshold__
    def set_dcData(self, data: pd.DataFrame) -> None: self.__dc_data__ = data
    def get_dcData(self) -> pd.DataFrame: return self.__dc_data__
