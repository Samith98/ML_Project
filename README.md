# ML_Project

This Repository contains the source code of the Machine Learning model for Titanic Dataset.
Below are the brief descriptions of the files and their utilizations in the project.

### datautils.py:
This file contains the Classes and methods that deal with below tasks
  - (class) DataOperator:
    - Performs extraction of data from/to the below storages and also contains the getters and setters to access the private variables of the class.
      - Database.
      - CSV file.
      - API
  - (class) DataCleaner:
    -  Fills the missing data (Imputation) and Treats the outlier values in the dataset (Winsorization)
    -  Drops the columns that have large number of missing data percentage.
  - Feature Engineering on the dataset
  - Store the processed data into the Database again.
