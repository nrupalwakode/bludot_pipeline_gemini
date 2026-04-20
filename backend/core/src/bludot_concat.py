import os
import datetime
import numpy as np
import pandas as pd

def date_formatting(dataset):
    date_formating_list = []
    dataset = dataset.replace('', np.nan)
    for cols_names in dataset.columns:
        if dataset[cols_names][dataset[cols_names].notnull()].shape[0] >= 1:
            col_data_type = type(dataset[cols_names][dataset[cols_names].notnull()].values[0])
        else:
            col_data_type = type(dataset[cols_names][dataset[cols_names].notnull()].values)

        if col_data_type == datetime.datetime:
            date_formating_list.append(cols_names)

    dataset.fillna('', inplace=True)
    for sel_cols_name in date_formating_list:
        action_taken_list = []
        for cols_values, cols_index in zip(dataset[sel_cols_name], dataset[date_formating_list].index):
            if type(cols_values) == str:
                action_taken_list.append(cols_values)
            else:
                action_taken_list.append(cols_values.strftime("%m/%d/%Y"))

        dataset[sel_cols_name] = action_taken_list

    date_lists = []
    for date_dtype_col_name in dataset.dtypes[dataset.dtypes == 'datetime64[ns]'].index:
        dataset[f'{date_dtype_col_name}'] = pd.to_datetime(
            dataset[f'{date_dtype_col_name}'], format='%Y%m%d').dt.strftime('%m/%d/%Y')
    dataset[date_lists] = dataset[date_lists].astype('object')

    # from datetime import datetime, timedelta

    # base_date = datetime(1900, 1, 1)
    # target_date = base_date + timedelta(days=42005)

    # formatted_date = target_date.strftime("%Y-%m-%d")
    # print(formatted_date)

    return dataset

#     import pandas as pd
# from datetime import datetime, timedelta
# import numpy as np

# def convert_date_columns(df):
#     base_date = datetime(1899, 12, 30)

#     date_columns = ['Approval Date', 'Start date', 'Membership Start Date', 'Issued Date', 'Start Date', 'Expiration Date']

#     for column in date_columns:
#         if column == 'Membership Start Date':
#             df[column] = df[column].apply(lambda x: base_date + timedelta(days=int(x)) if pd.notna(x) else pd.NaT)
#             df[column] = pd.to_datetime(df[column], errors='coerce').dt.strftime('%m/%d/%Y')
#         else:
#             df[column] = pd.to_datetime(df[column], errors='coerce').dt.strftime('%m/%d/%Y')

#     return df

# # Assuming your DataFrame is named 'df'
# df = convert_date_columns(df)

# # Printing the updated DataFrame
# print(df)
# df.to_excel('check_date.xlsx')

def bludot_concatenation(business_df, custom_df, contact_df, column_names):
    # Rename columns in the contact dataframe as after reading this excel if there is any duplicate col name it will add '.1','.2' but i want in original format for contact deduplication
    merged_df=business_df
    contact_col_names = []
    if not custom_df.empty:
        merged_df = pd.merge(business_df, custom_df, left_on=column_names[0], right_on=column_names[1])
    if not contact_df.empty:
        for col in contact_df.columns:
            if '.' in col:
                original_col_name= col.split(".")[0]
                contact_col_names.append(original_col_name)
            else:
                contact_col_names.append(col)
        contact_df.columns = contact_col_names

    # Merge the dataframes using the specified column names
    
        merged_df = pd.merge(merged_df, contact_df, left_on=column_names[0], right_on=column_names[2])

    return merged_df

def bludot_sheets_concatenation(city_path, raw_sheet_name, business_sheet, custom_sheet, contact_sheet):
    working_file_path = os.path.join(city_path, 'original_record', raw_sheet_name)
    uuid_column_names = []

    # business_records_update = None
    if business_sheet != '':
        business_records = pd.read_excel(working_file_path, sheet_name=business_sheet,dtype=object)
        business_records_update = date_formatting(dataset=business_records)
        uuid_column_names.append('UUID')

    # custom_records_update = None
    custom_records_update=pd.DataFrame()
    if custom_sheet != '':
        custom_records = pd.read_excel(working_file_path, sheet_name=custom_sheet,dtype=object)
        custom_records_update = date_formatting(dataset=custom_records)
        uuid_column_names.append('Custom Data Name')
        

    # contact_records_update = None
    contact_records_update=pd.DataFrame()
    if contact_sheet != '':
        contact_records = pd.read_excel(working_file_path, sheet_name=contact_sheet,dtype=object)
        contact_records_update = date_formatting(dataset=contact_records)
        uuid_column_names.append('ID')

    # Concatenate the records using the bludot_concatenation function
    bludot_concatenated_records = bludot_concatenation(business_df=business_records_update,
                                                      custom_df=custom_records_update,
                                                      contact_df=contact_records_update,
                                                      column_names=uuid_column_names)

    bludot_concatenated_records['bludot_index'] = bludot_concatenated_records.index
    return bludot_concatenated_records

