import pandas as pd
import numpy as np
import time
from configparser import ConfigParser
import mysql
from mysql.connector import Error, connect
from operatedb import DbOperator
from sklearn import preprocessing

class DataCleaner:
    # Private variable of the class DataCleaner
    __data__ = None
    __missing_threshold__ = None
    __default_col__ = None

    def __init__(self, columns=None, data=pd.DataFrame()):
        self.set_default_col(columns)
        self.__data__ = data
    
    # Method to impute the missing values
    def impute(self, method="median, mode", col=list) -> None:
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
            st = time.time()
            print("Imputing data with {0} method".format(method))
            method = method.split(",")
            for column in col:
                if self.__data__[column].dtype == "object":
                    if method[1].strip() == "mode":
                        self.__data__.fillna(value=self.__data__[column].mode()[0], inplace=True)
                else:
                    if method[0].strip() == "mean":
                        self.__data__[column].fillna(value=self.__data__[column].mean(), inplace=True)
                    if method[0].strip() == "median":
                        self.__data__[column].fillna(value=self.__data__[column].median(), inplace=True)
            print("Imputation Complete in {0:.2f} seconds.".format(time.time() - st))
            return self.get_data()
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
        missing_data = round(self.__data__.isna().sum()/self.__data__.shape[0] * 100, 2)
        missing_df = pd.DataFrame(data=missing_data.reset_index())
        missing_df.columns = ["Attribute", "Percentage"]
        #obj = DbOperator()
        #obj.push_to_db(table="missing_data", data=missing_df)
        return missing_df

    # Method to treat outliers
    def winsorize(self, method="IQR", col=list) -> None:
        method = method.lower()
        if col is not None:
            st = time()
            print("Started winsorization")
            if method == "iqr":
                for column in col:
                    quartile_one, quartile_two, quartile_three = np.percentile(self.__data__[column],  [25, 50, 75])
                    iqr = quartile_three - quartile_one
                    pos_out = quartile_three + 1.5 * iqr
                    neg_out = quartile_one - 1.5 * iqr
                    self.__data__[self.__data__[column] > pos_out] = quartile_three
                    self.__data__[self.__data__[column] < neg_out] = quartile_one
            print("Winsorization complete in {0:.2f} seconds.".format(time() - st))
            return self.get_data()
        else:
            print("Expected at least 1 column, but got None")
            return

    # Method to drop columns from DataFrame
    def drop_columns(self, col=list) -> None:
        if col is not None:
            self.__data__.drop(labels=col, axis=1, inplace=True)
            return self.get_data()
        else:
            print("Expected at least 1 column, got None")
            return

    # Getters and setters for the private variables of the class DataCleaner
    def set_default_col(self, col=None) ->  None: self.__default_col__ = col
    def get_default_col(self) -> list: return self.__default_col__
    def set_missing_threshold(self, val=int) -> None: self.__missing_threshold__ = val
    def get_missing_threshold(self) -> int: return self.__missing_threshold__
    def set_data(self, data=pd.DataFrame) -> None: self.__data__ = data
    def get_data(self) -> pd.DataFrame: return self.__data__


class DataScaler:
    __data__ = None

    def __init__(self, data=pd.DataFrame):
        self.__data__ = data

    def label_encoder(self, columns=list) -> None:
        try:
            if columns is not None:
                encode = preprocessiong.LableEncoder()
                for col in columns:
                    encode = encode.fit(self.__data__[col])
                    self.__data__[col] = encode.transform(self.__data__[col])
            else:
                print("Expected at least 1 column, got None")
        except Exception as e:
            print(e)

    def standard_scaler(self, columns=list) -> None:
        try:
            if columns is not None:
                sc = preprocessiong.StandardScaler()
                for col in columns:
                    sc = sc.fit(self.__data__[col])
                    self.__data__[col] = sc.transform(self.__data__[col])
            else:
                print("Expected at least 1 column, got None")
        except Exception as e:
            print(e)
    
    def min_max_scaler(self, columns=list) -> None:
        try:
            if columns is not None:
                mms = preprocessiong.MinMaxScaler(feature_range=(0, 2))
                for col in columns:
                    mms = mms.fit(self.__data__[col])
                    self.__data__[col] = mms.transform(self.__data__[col])
            else:
                print("Expected at least 1 column, got None")
        except Exception as e:
            print(e)