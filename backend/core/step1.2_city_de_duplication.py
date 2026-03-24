import os
import pandas as pd
from city_details import *
from src.de_duplication import fuzzy_deduplication

# Working city folder path
new_city_path = os.path.join(os.getcwd(),'cities_and_counties', CITY_NAME)

# Deduplication folder path
city_data_path=os.path.join(new_city_path, 'results', 'city_data')

th=1
manual_dedup_df=pd.read_excel(os.path.join(city_data_path,'manual_dedup_records.xlsx'))
deduplicated_sheet=fuzzy_deduplication(manual_dedup_df,city_data_path,th)

deduplicated_sheet.to_excel(os.path.join(city_data_path, 'de_duplication.xlsx'),
                                     sheet_name='De_Duplication',
                                     index=False)