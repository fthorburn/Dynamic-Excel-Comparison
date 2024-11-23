import pandas as pd

# A function to return the intersection between two lists as a list
def intersection(lst1, lst2):
    return list(set(lst1) & set(lst2))

# A function to compare dataFrames with inferred information about matching rows and columns
def discDf(df1: pd.DataFrame, df2: pd.DataFrame, selected_df: dict = None, keyVar: str = None):

    # Creating df labels
    fileName_label1 = "df1_value"
    fileName_label2 = "df2_value"

    # Inferring if rows are in identical order and number
    matchRow = (df1.shape[0] == df2.shape[0]) and (df1.index.equals(df2.index))

    # Inferring if column names match
    matchVarNames = set(df1.columns) == set(df2.columns)

    # Creating a discrepancy dataFrame
    disc_df = pd.DataFrame()

    if matchRow and matchVarNames:
        # Rows and columns match in number and order

        for i in range(df1.shape[0]):
            for col in df1.columns:
                if df1.loc[i, col] != df2.loc[i, col]:
                    new_row = {
                        "Index": i,
                        "Column_Name": col,
                        fileName_label1: df1.loc[i, col],
                        fileName_label2: df2.loc[i, col]
                    }
                    disc_df = pd.concat([disc_df, pd.DataFrame([new_row])], ignore_index=True)

    elif matchRow and not matchVarNames:
        # Rows match in number and order but columns have different names

        combined_df = pd.concat([df1, df2], axis=1)

        for i in range(combined_df.shape[0]):
            for long_col, short_col in selected_df.items():
                long_value = df1.loc[i, long_col]
                short_value = df2.loc[i, short_col]
                if long_value != short_value:
                    new_row = {
                        "Index": i,
                        "Column_Names": long_col + "/" + short_col,
                        fileName_label1: long_value,
                        fileName_label2: short_value
                    }
                    disc_df = pd.concat([disc_df, pd.DataFrame([new_row])], ignore_index=True)

    elif not matchRow and matchVarNames:
        # Rows do not match in number and order but columns match

        common_cols = intersection(df1.columns.values.tolist(), df2.columns.values.tolist())
        merged_df = pd.merge(df1, df2, on=keyVar, how="outer", suffixes=["_x", "_y"])

        for i in range(merged_df.shape[0]):
            for col in common_cols:
                x_value = merged_df.loc[i, col + "_x"]
                y_value = merged_df.loc[i, col + "_y"]
                if x_value != y_value:
                    new_row = {
                        "Key_Variable": merged_df.loc[i, keyVar],
                        "Merged_Index": i,
                        "Column_Name": col,
                        fileName_label1: x_value,
                        fileName_label2: y_value
                    }
                    disc_df = pd.concat([disc_df, pd.DataFrame([new_row])], ignore_index=True)

    else:
        # Rows and columns do not match

        df1 = df1.rename(columns={c: c + '_df1' for c in df1.columns if c not in [keyVar]})
        df2 = df2.rename(columns={c: c + '_df2' for c in df2.columns if c not in [keyVar]})

        merged_df = pd.merge(df1, df2, on=keyVar, how="outer")

        for i in range(merged_df.shape[0]):
            for long_col, short_col in selected_df.items():
                long_value = merged_df.loc[i, long_col + '_df1']
                short_value = merged_df.loc[i, short_col + '_df2']
                if long_value != short_value:
                    new_row = {
                        "Key_Variable": merged_df.loc[i, keyVar],
                        "Merged_Index": i,
                        "Column_Names": long_col + "/" + short_col,
                        fileName_label1: long_value,
                        fileName_label2: short_value
                    }
                    disc_df = pd.concat([disc_df, pd.DataFrame([new_row])], ignore_index=True)

    return disc_df
