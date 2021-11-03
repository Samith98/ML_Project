from datautils import DataCleaner

if __name__ == "__main__":
    obj = DataCleaner()
    if obj.get_data_flag():
        obj.set_dcData(obj.pull_data_from_db(table_name='titanic'))
        print(obj.get_dcData())