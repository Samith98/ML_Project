from datautils import DataExtractor

if __name__ == "__main__":
    obj = DataExtractor()
    df = obj.pull_data_from_db(table_name='titanic')
    print(df)