import pandas as pd

# test set 1
data1 = [
    {"1col1": 1, "1col2": "A"},
    {"1col1": 9999, "1col2": "B"},
    {"1col1": 3, "1col2": "A"},
    {"1col1": 4, "1col2": "C"},
]
data2 = [
    {"2col1": 1, "2col2": "A"},
    {"2col1": 2, "2col2": "B"},
    {"2col1": 3, "2col2": "A"},
    {"2col1": 99999, "2col2": "C"},
]
selected_df_data = [
    {"longColNames": "1col1", "shortColNames": "2col1"},
    {"longColNames": "1col2", "shortColNames": "2col2"},
    {"longColNames": "1col3", "shortColNames": "Do Not Compare"}
]
# Making sample data dataFrames
df1_test = pd.DataFrame(data1)
df2_test = pd.DataFrame(data2)
selected_df_test = pd.DataFrame(selected_df_data)
# Add a column of data to data1 called 1col3 with value 0
df1_test['1col3'] = 0

#test set 2
df3_test = pd.DataFrame({'col1': [1,2,3,4,5], 'col2': [5,4,3,2,1]})
df4_test = pd.DataFrame({'col1': [1,2,3,4,999], 'col2': [999,4,3,2,1]})

# Create sample data
data5 = {'col1': [1, 2, 3, 4], 'col2': ['A', 'B', 'A', 'C'], 'shared_var': ['X', 'Y', 'X', 'Z']}
data6 = {'col1': [10, 11, 12], 'col2': ['D', 'E', 'F'], 'shared_var': ['X', 'Y', 'W']}
# Create DataFrames
df_test5 = pd.DataFrame(data5)
df_test6 = pd.DataFrame(data6)

# Create a list of data
data9 = [[42864842, 13, 6, 8, 8],
        [49029542, 3, 6, 5, 5],
        [13844300, 2, 4, 7, 7]]
# Create column names
col_names9 = ['ID','1col1_#','1col2','1col3','1col 4']

# Create the DataFrame
df_test9 = pd.DataFrame(data9, columns=col_names9)

# Create a list of data
data10 =[[42864842, 13, 6, 8, 8],
         [49029542, 3, 6, 5, 5],
         [13844300, 2, 4, 7, 7],
         [99999999, 3, 4, 4, 8]]
# Create column names
col_names10 = ['ID','1col1_#','2col2','2col3','2col4']
# Create the DataFrame
df_test10 = pd.DataFrame(data10, columns=col_names10)
# Assign the name 'df_test10' to the DataFrame 
df_test10.name = 'df_test10'

#print(df_test10)

# Create a list of dictionaries containing the data
data_sdf = [
    {"longColNames": "1col1_#", "shortColNames": "1col1_#", "VarNum": "Var 1"},
    {"longColNames": "1col2", "shortColNames": "2col2", "VarNum": "Var 2"},
    {"longColNames": "1col3", "shortColNames": "2col3", "VarNum": "Var 3"},
    {"longColNames": "1col 4", "shortColNames": "Do Not Compare", "VarNum": "Var 4"},
]
# Create the DataFrame using pd.DataFrame()
df_sdf = pd.DataFrame(data_sdf)
#print(df_sdf)






#-------------------------------------------------------------------------------------------------------------------------------------------------------------

# A function to return the intersection between two lists as a list
def intersection(lst1, lst2):
    return list(set(lst1) & set(lst2))

# A function to compare dataFrames given a dictionary matching up their names, and a key variable to merge by (if necessary)
def discDf(df1: pd.DataFrame, df2: pd.DataFrame, selected_df: pd.DataFrame = None,matchRow: bool = None, matchVarNames: bool = None, keyVar: str = None):

    # Creating df labels
    fileName_label1 = "first_df_value"
    fileName_label2 = "second_df_value"

    # Insufficient information about dataFrames
    if matchRow is None and matchVarNames is None:
        return TypeError

    # Only call df if it exists
    if selected_df is not None:
        # Filtering out rows containing "Do Not Compare" in either columns
        selected_df = selected_df[selected_df['shortColNames'] != "Do Not Compare"]
        selected_df = selected_df[selected_df['longColNames'] != "Do Not Compare"]

    # Creating a discr. dataFrame and counter 
    disc_df = pd.DataFrame()

    # Returns a discrepancy dataFrame when data is in identical number and order rows, and the variable names are not identical 
    if matchRow is True and matchVarNames is False:

        # Adding suffixes to all column names
        df1 = df1.add_suffix("_long")
        df2 = df2.add_suffix("_short")

        # Concatonating dfs for easier comparison
        combined_df = pd.concat([df1, df2], axis=1)

        # Comparing data frames
        for i in range(combined_df.shape[0]):   # Iterating through concatonated dataFrame, data from both files
            for index, select_col in selected_df.iterrows(): # Iterating through dataFrame containing matched up column names

                # Getting column names in the same row 
                long_col = select_col.iloc[0] + '_long'
                short_col = select_col.iloc[1] + '_short'

                # Getting value from long df and short df for comparison
                long_value = combined_df.loc[i, long_col]
                short_value = combined_df.loc[i, short_col]

                # Building discrepancies table
                if long_value != short_value: 

                    # Creating a new discrepancy row
                    new_row = {
                        "Index": i, # Location in combined df, no need to translate to orginal dataframes since this is under assumption that the rows are identical in order/number
                        "Column_Names": select_col.iloc[0] + "/" + select_col.iloc[1], # Showing both column names without suffix
                        fileName_label1: long_value,  # Dynamically update file name
                        fileName_label2: short_value  # Dynamically update file name
                    }

                    # Adding new discrepancy row to discrepancy table
                    disc_df = pd.concat([disc_df, pd.DataFrame([new_row])], ignore_index=True)

                    #returning discrepancy df
                    return disc_df

    # Returns a discrepancy dataFrame when data is in identical number and order rows, and the variable names are identical
    if matchRow is True and matchVarNames is True:

        # Getting column names from both data frames
        colNames1= df1.columns.values.tolist()
        colNames2= df2.columns.values.tolist()

        # Get all column names shared between both dataFrames
        common_cols = intersection(colNames1, colNames2)

        # Adding suffixes to all column names
        df1 = df1.add_suffix("_long")
        df2 = df2.add_suffix("_short")

        # Concatonating dataFrames
        combined_df = pd.concat([df1,df2], axis=1)

        for i in range(combined_df.shape[0]):
            for col in common_cols:

                # Getting value from long df and short df for comparison
                long_value = combined_df.loc[i,col + "_long"]
                short_value = combined_df.loc[i,col + "_short"]

                if long_value != short_value:

                    # Creating a new discrepancy row
                    new_row = {
                        "Index": i, # Location in combined df, no need to translate to orginal dataframes since this is under assumption that the rows are identical in order/number
                        "Column_Name": col,
                        fileName_label1: long_value,  # Dynamically update file name
                        fileName_label2: short_value  # Dynamically update file name
                    }

                    # Adding new discrepancy row to discrepancy table
                    disc_df = pd.concat([disc_df, pd.DataFrame([new_row])], ignore_index=True)

        #returning discrepancy df
        return disc_df

    # Returns a discrepancy dataFrame when data is not in identical number and ordered rows, and the variable names are identical
    if matchRow is False and matchVarNames is True:
        
        # Getting column names from both data frames
        colNames1= df1.columns.values.tolist()
        colNames2= df2.columns.values.tolist()

        # Get all column names shared between both dataFrames, besides keyVar
        common_cols = [col for col in set(colNames1) & set(colNames2) if col != keyVar]

        # Merging dataFrames by keyVar
        merged_df = pd.merge(df1, df2, on=keyVar, how="outer", suffixes=["_x","_y"])

        for i in range(merged_df.shape[0]):
            for col in common_cols:

                # Getting value from long df and short df for comparison
                x_value = merged_df.loc[i,col + "_x"]
                y_value = merged_df.loc[i,col + "_y"]

                if x_value != y_value:

                    new_row = {
                        "Key_Variable": merged_df.loc[i, keyVar], # value of identifying row variable shared between dataFrames
                        "Merged_Index": i, # Location in merged df, not original dataFrames
                        "Column_Name": col,
                        fileName_label1: x_value, 
                        fileName_label2: y_value  
                    }

                    # Adding new discrepancy row to discrepancy table
                    disc_df = pd.concat([disc_df, pd.DataFrame([new_row])], ignore_index=True)

        return disc_df
    
    # Returns a discrepancy dataFrame when data is not in identical number and ordered rows, and the variable names are not identical
    if matchRow is False and matchVarNames is False:

        #compare_ff(df1,df2,keyVar=keyVar)
        # Adding suffixes to all column names except key variable we merge by
        df1 = df1.rename(columns={c: c+'_x' for c in df1.columns if c not in [keyVar]})
        df2 = df2.rename(columns={c: c+'_y' for c in df2.columns if c not in [keyVar]})

        # Merging dataFrames by keyVar
        merged_df = pd.merge(df1, df2, on=keyVar, how="outer")

        # Comparing data frames
        for i in range(merged_df.shape[0]):   # Iterating through concatonated dataFrame, data from both files
            for index, select_col in selected_df.iterrows(): # Iterating through dataFrame containing matched up column names

                # Getting column names in the same row 
                x_col = select_col.iloc[0] + '_x'
                y_col = select_col.iloc[1] + '_y'

                # Getting value from long df and short df for comparison
                x_value = merged_df.loc[i, x_col]
                y_value = merged_df.loc[i, y_col]

                if x_value != y_value:

                    new_row = {
                        "Key_Variable": merged_df.loc[i, keyVar], # value of identifying row variable shared between dataFrames
                        "Merged_Index": i, # Location in merged df, not original dataFrames
                        "Column_Names": select_col.iloc[0] + "/" + select_col.iloc[1],
                        fileName_label1: x_value, 
                        fileName_label2: y_value  
                    }

                    # Adding new discrepancy row to discrepancy table df
                    disc_df = pd.concat([disc_df, pd.DataFrame([new_row])], ignore_index=True)

        return disc_df
    

#def compare_ff(df1: pd.DataFrame, df2: pd.DataFrame, selected_df: pd.DataFrame = None,matchRow: bool = None, matchVarNames: bool = None, keyVar: str = None):
    #print("compareff")

    # # Adding suffixes to all column names except key variable we merge by
    # df1 = df1.rename(columns={c: c+'_x' for c in df1.columns if c not in [keyVar]})
    # df2 = df2.rename(columns={c: c+'_y' for c in df2.columns if c not in [keyVar]})





#-------------------------------------------------------------------------------------------------------------------------------------------------------------


# maybe make a class, with a method that returns a discrepancy table
# another method that returns the discrepancy count 

# works!
#print(discDf(df1= df1_test, df2=df2_test, matchRow=True, matchVarNames=False, selected_df=selected_df_test))

# works!
#print(discDf(df3_test, df4_test, matchRow=True, matchVarNames=True))

#print(df_test5)
#print(df_test6)
# i think works!
#print(discDf(df_test5, df_test6, matchRow=False, matchVarNames=True, keyVar="shared_var"))


print(discDf(df_test9, df_test10, matchRow=False, matchVarNames=False, selected_df=df_sdf, keyVar='ID'))
# alter the test data in this one to include a keyVar to match up the rows




#use pytest to test your code
