# from datautils import DataCleaner
from operatedb import DbOperator

if __name__ == "__main__":
    obj = DbOperator()
    obj.create_table(name="test", column=("Attribute", "Percentage"), column_type=("text", "int"))
    obj.push_to_db(table="test", values=["q"])