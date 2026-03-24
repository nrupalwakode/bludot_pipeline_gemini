import os
import shutil
from city_details import *
from src.bludot_concat import bludot_sheets_concatenation
from src.de_duplication import city_de_duplication


if __name__ == "__main__":
    print(f"""---------------------------------------------------------------------------------------------
                    Step1: Creating Working Directory For {CITY_NAME}                    
        ---------------------------------------------------------------------------------------------""")
    # Define variables

    new_city_dir = os.path.join(os.getcwd(), "cities_and_counties", CITY_NAME)
    original_record_dir = os.path.join(new_city_dir, "original_record")
    results_dir = os.path.join(new_city_dir, "results")
    bludot_data_dir = os.path.join(results_dir, "bludot_data")
    city_data_dir = os.path.join(results_dir, "city_data")

    # Create work directory if it doesn't exist
    if not os.path.exists(new_city_dir):
        os.mkdir(new_city_dir)
        print(f"Working Directory for {CITY_NAME} is created")
    else:
        print(f"Working directory for {CITY_NAME} already exists")

    # Create Original Records directory if it doesn't exist
    if not os.path.exists(original_record_dir):
        os.mkdir(original_record_dir)
        print("Created Original Records Directory inside Work Directory")

    # Create Results directory if it doesn't exist
    if not os.path.exists(results_dir):
        os.mkdir(results_dir)
        print("Created Results Directory inside Work Directory")


    # Copy raw data sheet to Original Record directory
    src_path = os.path.join(os.getcwd(), "raw_data", RAW_DATA_SHEET_NAME)
    dest_path = os.path.join(original_record_dir, RAW_DATA_SHEET_NAME)
    shutil.copy(src_path, dest_path)


    print("""---------------------------------------------------------------------------------------------
                    Step2: Performing De-Duplication on City Records and Concatenating Bludot Sheets
    ---------------------------------------------------------------------------------------------""")

    # Create Bludot Data directory if it doesn't exist
    if not os.path.exists(bludot_data_dir):
        os.mkdir(bludot_data_dir)
        print("Bludot Data folder is created")

    # Perform Bludot sheets concatenation
    bludot_concatenated_records=bludot_sheets_concatenation(city_path = new_city_dir,
                    raw_sheet_name     = RAW_DATA_SHEET_NAME,
                    business_sheet     = BLUDOT_BUSINESS_SHEET_NAME,
                    custom_sheet       = BLUDOT_CUSTOM_SHEET_NAME,
                    contact_sheet      = BLUDOT_CONTACT_SHEET_NAME)

    # Save Bludot Concatenated records to Excel
    bludot_concatenated_records.to_excel(os.path.join(bludot_data_dir,"bludot_concatenated_records.xlsx"),
                                        sheet_name="Bludot_Concatenated_Records",
                                        index=False)

    print("Bludot concatenated sheet is created \n")

    # Create De-Duplication directory if it doesn't exist
    if not os.path.exists(city_data_dir):
        os.mkdir(city_data_dir)
        print("De-duplication folder is created")

    # Perform city de-duplication
    # city_de_duplication(city_path = new_city_dir,
    #                 raw_sheet_name     = RAW_DATA_SHEET_NAME,
    #                 city_sheet         = CITY_SHEET_NAME,
    #                 dedup_columns_list = DEDUPLICATION_COLUMN_LIST)

    print("Manual Check sheet for deduplication is created inside city_data folder")
    print("---------------------------------------------------------------------------------------------")

    #Remove settings and training file as I want to retrain for each city
    if os.path.exists(os.path.join(os.getcwd(), "dedupe_dataframe_learned_settings")):
        os.remove(os.path.join(os.getcwd(), "dedupe_dataframe_learned_settings"))
        os.remove(os.path.join(os.getcwd(), "dedupe_dataframe_training.json"))



