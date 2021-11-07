from datautils import DataCleaner, DataScaler
from operatedb import DbOperator
import pandas as pd

if __name__ == "__main__":
    data_df = pd.read_csv(r"E:\Github Code Base\ML_Project\titanic.csv")
    obj = DataCleaner(data=data_df, columns=["Survived"])
    data_df = obj.impute(col=["Age", "Embarked"])
    data_df = obj.drop_columns(col=["Cabin"])
    print(data_df.columns)
    print(obj.get_missing_data_percent())