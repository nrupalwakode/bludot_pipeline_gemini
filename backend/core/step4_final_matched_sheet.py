import os
import pandas as pd
import numpy as np
from city_details import *

# Defining File Path & Location 
#------------------------------------------------------------------------------------------------------------------------------------------------
new_city_path = os.path.join(os.getcwd(),'cities_and_counties', CITY_NAME)

#------------------------------------------------------------------------------------------------------------------------------------------------
city_records   = pd.read_excel(os.path.join(new_city_path,'results','city_data','de_duplication_merged.xlsx'))
bludot_records = pd.read_excel(os.path.join(new_city_path,'results','bludot_data','bludot_concatenated_records.xlsx'))

if not os.path.exists(os.path.join(new_city_path,'results','output')):
    os.mkdir(os.path.join(new_city_path,'results','output'))


def separate_main_spreadsheet(original_data, city_dataset, bludot_dataset):

    additional_city_dataset = city_dataset[~city_dataset.city_index.isin(
        original_data['city_index'].values)]
    additional_bludot_dataset = bludot_dataset[~bludot_dataset.bludot_index.isin(
        original_data['bludot_index'].values)]
    return additional_city_dataset, additional_bludot_dataset
    
print("Step6: Data Recreation For Excel Creation")
file_loc_path = os.path.join(new_city_path,'results','output') #output
updated_excel_sheet = pd.read_excel(os.path.join(new_city_path,'results','filter_matches',f'Updated_Matched_Records.xlsx')) 

additional_city_dataset,additional_bludot_dataset = separate_main_spreadsheet(original_data=updated_excel_sheet,
                                                                              city_dataset=city_records,
                                                                              bludot_dataset=bludot_records)

if not os.path.exists(os.path.join(new_city_path,'results','output','final_excel')):
    os.mkdir(os.path.join(new_city_path,'results','output','final_excel'))

if not os.path.exists(os.path.join(new_city_path,'results','output','final_result')):
    os.mkdir(os.path.join(new_city_path,'results','output','final_result'))
      
if not os.path.exists(os.path.join(new_city_path,'results','output','final_output')):      
     os.mkdir(os.path.join(new_city_path,'results','output','final_output'))

updated_excel_sheet.to_excel(os.path.join(file_loc_path,'final_result',f'final_matched_records_for_{CITY_NAME}.xlsx'),
                             index=False)
                             
additional_city_dataset.to_excel(os.path.join(file_loc_path,'final_result',f'additional_city_records_for_{CITY_NAME}.xlsx'),
                             index=False)

additional_bludot_dataset.to_excel(os.path.join(file_loc_path,'final_result',f'additional_bludot_records_for_{CITY_NAME}.xlsx'),
                             index=False)
#------------------------------------------------------------------------------------------------------------------------------------------------
print("Step7: Excel Formating work directory creation")
#------------------------------------------------------------------------------------------------------------------------------------------------
