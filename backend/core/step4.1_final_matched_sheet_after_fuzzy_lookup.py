import os
import pandas as pd
import numpy as np
from city_details import *
from step4_final_matched_sheet import separate_main_spreadsheet

# Defining File Path & Location 
#------------------------------------------------------------------------------------------------------------------------------------------------
new_city_path = os.path.join(os.getcwd(),'cities_and_counties', CITY_NAME)

#------------------------------------------------------------------------------------------------------------------------------------------------
city_records   = pd.read_excel(os.path.join(new_city_path,'results','city_data','de_duplication_merged.xlsx'))
bludot_records = pd.read_excel(os.path.join(new_city_path,'results','bludot_data','bludot_concatenated_records.xlsx'))

# read matched city and bludot index after fuzzy lookup on additional_city_records and additional_bludot_records
matched_city_bludot_index = pd.read_excel(os.path.join(new_city_path,'results','filter_matches','city_bludot_index.xlsx'))
# print(matched_city_bludot_index.city_index)



def separate_main_spreadsheet_new(original_data, city_dataset, bludot_dataset):

    additional_city_dataset = city_dataset[city_dataset.city_index.isin(
        original_data['city_index'].values)]
    additional_bludot_dataset = bludot_dataset[bludot_dataset.bludot_index.isin(
        original_data['bludot_index'].values)]
    return additional_city_dataset, additional_bludot_dataset
    
print("Step6: Data Recreation For Excel Creation")
file_loc_path = os.path.join(new_city_path,'results','output') #output
updated_excel_sheet = pd.read_excel(os.path.join(new_city_path,'results','filter_matches',f'Updated_Matched_Records.xlsx')) 

additional_city_dataset,additional_bludot_dataset = separate_main_spreadsheet_new(original_data=matched_city_bludot_index,
                                                                              city_dataset=city_records,
                                                                              bludot_dataset=bludot_records)
# Assuming 'df' is your DataFrame and 'specific_column' is the column based on which you want to rearrange
# 'given_list' is the list of values in the specific column to use for reordering

given_list =  matched_city_bludot_index['city_index'] # Your given list of values
specific_column = 'city_index'  # Replace 'column_name' with the actual column name

# Create a dictionary to map values to their corresponding indices in the given list
value_index_map = {value: index for index, value in enumerate(given_list)}

# Use the map to get the indices for reindexing
reindex_order = additional_city_dataset[specific_column].map(value_index_map)

# Rearrange the DataFrame based on the reindex order
rearranged_df = additional_city_dataset.iloc[reindex_order.argsort()]

given_list_2 =  matched_city_bludot_index['bludot_index'] # Your given list of values
specific_column_2 = 'bludot_index'  # Replace 'column_name' with the actual column name

# Create a dictionary to map values to their corresponding indices in the given list
value_index_map_2 = {value: index for index, value in enumerate(given_list_2)}

# Use the map to get the indices for reindexing
reindex_order_2 = additional_bludot_dataset[specific_column_2].map(value_index_map_2)

# Rearrange the DataFrame based on the reindex order
rearranged_df_2 = additional_bludot_dataset.iloc[reindex_order_2.argsort()]

rearranged_df.to_excel(os.path.join(file_loc_path,'final_result',f'fuzzy_matched_city_records_for_{CITY_NAME}.xlsx'),
                        index=False)

rearranged_df_2.to_excel(os.path.join(file_loc_path,'final_result',f'fuzzy_matched_bludot_records_for_{CITY_NAME}.xlsx'),
                        index=False)

updated_excel_sheet = pd.read_excel(os.path.join(new_city_path,'results','filter_matches',f'Updated_Matched_Records.xlsx')) 

fuzzy_matched_city_records = pd.read_excel(os.path.join(file_loc_path,'final_result',
                                                        f'fuzzy_matched_city_records_for_{CITY_NAME}.xlsx'))
#print('fuzzzy city matched:',fuzzy_matched_city_records)
#print('len of city:',fuzzy_matched_city_records.shape[0])
#fuzzy_matched_city_records_except_city_index = fuzzy_matched_city_records.iloc[:, :-1]

fuzzy_matched_bludot_records = pd.read_excel(os.path.join(file_loc_path,'final_result',
                                                 f'fuzzy_matched_bludot_records_for_{CITY_NAME}.xlsx'))

merged_fuzzy_matched_city_bludot_records = pd.concat([fuzzy_matched_city_records,fuzzy_matched_bludot_records],axis = 1)

bludot_index_loc = merged_fuzzy_matched_city_bludot_records.columns.get_loc('bludot_index')
merged_fuzzy_matched_city_bludot_records.insert(bludot_index_loc -1,'city_index',
                                                merged_fuzzy_matched_city_bludot_records.pop('city_index'))
#print(type(merged_fuzzy_matched_city_bludot_records))
# fuzzy_matched_merged_city_bludot_records=merged_fuzzy_matched_city_bludot_records.to_excel(os.path.join(new_city_path,'results','filter_matches',
#                         f'fuzzy_matched_merged_city_bludot_records.xlsx'),index = False)

# Create a dataframe having fixed first 5 columns in dataset i.e UUID, name and address of city and bludot
uuid_name_address_city_bludot_df = pd.DataFrame()
uuid_name_address_city_bludot_df['UUID'] = merged_fuzzy_matched_city_bludot_records['UUID']
uuid_name_address_city_bludot_df[CITY_NAME_LIST[0]] = merged_fuzzy_matched_city_bludot_records[CITY_NAME_LIST[0]]
uuid_name_address_city_bludot_df[BLUDOT_NAME] = merged_fuzzy_matched_city_bludot_records[BLUDOT_NAME]
uuid_name_address_city_bludot_df[CITY_ADDRESS_LIST[0]] = merged_fuzzy_matched_city_bludot_records[CITY_ADDRESS_LIST[0]]
uuid_name_address_city_bludot_df[BLUDOT_ADDRESS] = merged_fuzzy_matched_city_bludot_records[BLUDOT_ADDRESS]
#print(type(uuid_name_address_city_bludot_df))
df_5columns_citybludot_merged = pd.concat([uuid_name_address_city_bludot_df,merged_fuzzy_matched_city_bludot_records],axis=1)
df_5columns_citybludot_merged.to_excel(os.path.join(new_city_path,'results','filter_matches',f'Updated_Matched_Records_Final.xlsx'),index=False)
#print(df_5columns_citybludot_merged)

# appending addtional matched data to Updated_Matched_Records file

additional_matched_records = pd.read_excel(os.path.join(new_city_path,'results','filter_matches',f'Updated_Matched_Records_Final.xlsx'))

#updated_excel_sheet = updated_excel_sheet.append(additional_matched_records, ignore_index = True)
updated_excel_sheet = pd.concat([updated_excel_sheet, additional_matched_records], ignore_index=True)

updated_excel_sheet.to_excel(os.path.join(new_city_path,'results','filter_matches',f'Updated_Matched_Records.xlsx'),index = False)

#------------------------------------------------------------------------------------------------------------------------------------------------
print("Step7: Running Step 4 again")
#------------------------------------------------------------------------------------------------------------------------------------------------

additional_city_dataset,additional_bludot_dataset = separate_main_spreadsheet(original_data=updated_excel_sheet,
                                                                              city_dataset=city_records,
                                                                              bludot_dataset=bludot_records)


updated_excel_sheet.to_excel(os.path.join(file_loc_path,'final_result',f'final_matched_records_for_{CITY_NAME}.xlsx'),
                             index=False)

additional_city_dataset.to_excel(os.path.join(file_loc_path,'final_result',f'additional_city_records_for_{CITY_NAME}.xlsx'),
                             index=False)

additional_bludot_dataset.to_excel(os.path.join(file_loc_path,'final_result',f'additional_bludot_records_for_{CITY_NAME}.xlsx'),
                             index=False)
