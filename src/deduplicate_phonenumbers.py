import pandas as pd
import re
import os

def standardize_phone(phone):
    """
    Standardize phone number by removing formatting characters.
    Returns None if the phone number is empty or None.
    """
    if pd.isna(phone) or phone == '':
        return None
    
    # Remove all non-digit characters
    phone_digits = re.sub(r'\D', '', phone)
    
    # Remove leading '1' if present (country code)
    if phone_digits.startswith('1') and len(phone_digits) > 10:
        phone_digits = phone_digits[1:]
        
    return phone_digits

def column_to_index(col_name):
    """
    Convert Excel column name (e.g., 'A', 'Z', 'AA', 'AZ') to a 0-based index.
    """
    result = 0
    for char in col_name:
        result = result * 26 + (ord(char) - ord('A') + 1)
    return result - 1

def index_to_column(index):
    """
    Convert a 0-based index to Excel column name (e.g., 0->'A', 25->'Z', 26->'AA')
    """
    result = ""
    index += 1  # Make it 1-based
    while index > 0:
        remainder = (index - 1) % 26
        result = chr(ord('A') + remainder) + result
        index = (index - 1) // 26
    return result

def deduplicate_phone_numbers(df, start_col='AG', end_col='AJ', output_col='AK'):
    """
    Deduplicates phone numbers across columns and joins them with comma and space.
    
    Parameters:
    - df: pandas DataFrame containing the data
    - start_col: Starting column name (e.g., 'AG')
    - end_col: Ending column name (e.g., 'AJ')
    - output_col: Column name where the result will be stored
    
    Returns:
    - DataFrame with the deduplicated phone numbers
    """
    # Convert column names to indices
    start_idx = column_to_index(start_col)
    end_idx = column_to_index(end_col)
    
    # Get column names
    cols = [index_to_column(i) for i in range(start_idx, end_idx + 1)]
    
    # Process each row
    for idx in range(len(df)):
        # Get phone numbers from the specified columns
        phone_numbers = []
        for col in cols:
            if col in df.columns:
                phone = df.iloc[idx, df.columns.get_loc(col)]
                if not pd.isna(phone) and phone != '':
                    phone_numbers.append(str(phone))
        
        # Create a mapping of standardized to original phone numbers
        phone_dict = {}
        for phone in phone_numbers:
            std_phone = standardize_phone(phone)
            if std_phone and std_phone not in phone_dict:
                phone_dict[std_phone] = phone
        
        # Join unique phone numbers with comma and space
        unique_phones = list(phone_dict.values())
        result = ', '.join(unique_phones)
        
        # Store result in the output column
        if output_col in df.columns:
            df.iloc[idx, df.columns.get_loc(output_col)] = result
        else:
            # Create the column if it doesn't exist
            df[output_col] = ""
            df.iloc[idx, df.columns.get_loc(output_col)] = result
    
    return df

# Example usage
if __name__ == "__main__":
    file_path = 'D:\\bludot_data_axle_refreshpagination\\bludot_data_matching\\new_city_onboarding\\cities_and_counties\\Redmond_Deduplication_25_04_2025\\results\\city_data\\de_duplication.xlsx'
    sheet_name = 'De_Duplication_Copy'
    
    # Read the Excel file
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    
    # Process the data - specify your actual column names here
    result_df = deduplicate_phone_numbers(df, start_col='AG', end_col='AJ', output_col='AK')
    
    # Create output file path properly
    dir_name = os.path.dirname(file_path)
    base_name = os.path.basename(file_path)
    output_path = os.path.join(dir_name, f"processed_{base_name}")
    
    # Save the result back to Excel
    result_df.to_excel(output_path, sheet_name=sheet_name, index=False)
    print(f"Processed file saved as '{output_path}'")