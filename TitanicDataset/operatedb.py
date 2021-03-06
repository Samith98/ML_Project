import pandas as pd
from configparser import ConfigParser
from mysql import connector
from mysql.connector import Error

class DbOperator:
    config = ConfigParser()
    config.read("config.ini")
    __sql_info__ = None
    __login__ = None
    __host__ = None
    __database__ = None
    __user__ = None
    __password__ = None
    
    def __init__(self):
        self.__sql_info__ = self.config["SQLServer"]
        self.__login__ = self.config["UserInfo"]
        self.__host__ = self.__sql_info__["host"]
        self.__database__ = self.__sql_info__["database"]
        self.__user__ = self.__login__["user"]
        self.__password__ = self.__login__["password"]

    def create_table(self, name=str(), column=list(), column_type=list()) -> None:
        if bool(name) and bool(column) and bool(column_type):
            try:
                if len(column) == len(column_type):
                    columns = " ".join("{0} {1},".format(column[i], column_type[i]) for i in range(0, len(column)))
                    table_query = "CREATE TABLE IF NOT EXISTS {0}({1})".format(name, columns[:-1])
                    con, cur = self.db_connect()
                    cur.execute(table_query)
                    print("Created table {0}".format(name))
                    con.close()
                else:
                    print("Length mismatch between column and column type")
            except Error as e:
                print(e)
        else:
            print("Expected at least 1, got None")

    def db_connect(self) -> tuple:
        if (self.__host__ is not None) and (self.__database__ is not None) and \
            (self.__user__ is not None) and (self.__password__ is not None):
            conn = connector.connect(host=self.__host__, database=self.__database__, user=self.__user__, password=self.__password__)
            if conn.is_connected():
                cur = conn.cursor()
                cur.execute("SELECT DATABASE()")
                db =  cur.fetchone()
                print("Connected to {0} database".format(db[0]))
                return (conn, cur)
            else:
                print("Not connected to Database")
        else:
            print("Connection parameters missing. Check the configuration")

    def get_dtype(val=str) -> str():
        data_dict = {
                "int64": "int",
                "int32": "int",
                "float64": "float",
                "float32": "float",
                "object": "text"
            }
        return data_dict.get(val)

    def push_to_db(self, table=None, data=None) -> None:
        try:
            if table is not None and data is not None:
                columns = data.columns.tolist()
                col_type = list()
                for col in columns:
                    col_type.append(self.get_dtype(str(data[col].dtype)))
                #col_type = [self.get_dtype(val=str(data[col].dtype)) for col in columns]
                self.create_table(name=table, column=columns, column_type=col_type)
                init_check = "SHOW COLUMNS from {0}.{1}".format(self.__database__, table)
                con, cur = self.db_connect()
                cur.execute(init_check)
                col_len = len(cur.fetchall())
                val_len = ""
                if col_len == val_len:
                    values = " ".join("%s" for _ in range(0, len(columns)))
                    for val in data.iterrows():
                        insert_query = "INSERT INTO {0} VALUES({1})".format(name, values)
                        con, cur = self.db_connect()
                        cur.execute(insert_query, val)
                    con.close()
                else:
                    print("Required {0} values, got {1}".format())
                con.close()
            else:
                print("Expected at least 1, got None")
        except Error as e:
            print(e)

    def pull_from_db(self, table=str()) -> pd.DataFrame:
        try:
            data_dict = dict()
            sql = "SELECT * FROM {0}".format(table)
            con, cur = self.db_connect()
            cur.execute(sql)
            data = cur.fetchall()
            data_dict["data"] = data
            sql2 = "SHOW COLUMNS from {0}.{1}".format(self.__database__, table)
            cur.execute(sql2)
            columns = cur.fetchall()
            columns = [columns[i][0] for i in range(0, len(columns))]
            data_dict["columns"] = columns
            data_df = pd.DataFrame(data=data_dict["data"], columns=data_dict["columns"])
            return data_df
        except Error as e:
            print(e)