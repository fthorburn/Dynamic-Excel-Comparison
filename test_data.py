from function import *
import pandas as pd

# Creating data for test set 9
data9 = [
    {"ID": 1, "Name": "Alice", "Age": 25},
    {"ID": 2, "Name": "Bob", "Age": 30},
    {"ID": 4, "Name": "David", "Age": 40},
    {"ID": 3, "Name": "Charlie", "Age": 35},
    {"ID": 5, "Name": "HBBHbh", "Age": 99}
]

# Creating data for test set 10
data10 = [
    {"ID": 1, "FullName": "Alice", "Age": 25},
    {"ID": 3, "FullName": "Charlie", "Age": 35},
    {"ID": 4, "FullName": "David", "Age": 45},
    {"ID": 2, "FullName": "Robert", "Age": 30},
]

# Dictionary to map column names
col_mapping = {
    "Name": "FullName",
    "Age": "Age"
}

# Converting lists to dataframes
df_test9 = pd.DataFrame(data9)
df_test10 = pd.DataFrame(data10)

# Function call with corrected parameter name
result = discDf(df_test9, df_test10, selected_df=col_mapping, keyVar='ID')
print(result)

