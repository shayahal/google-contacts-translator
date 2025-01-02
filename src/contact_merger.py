import pandas as pd
from deep_translator import GoogleTranslator

def is_hebrew(text):
    if not isinstance(text, str):
        return False
    hebrew_range = range(0x590, 0x5FF)
    return any(ord(char) in hebrew_range for char in text)

def translate_text(text):
    if not isinstance(text, str):
        return text
    if not is_hebrew(text):
        return text
    
    try:
        translator = GoogleTranslator(source='iw', target='en')
        return translator.translate(text)
    except Exception as e:
        print(f"Translation error for text '{text}': {str(e)}")
        return text

def process_contacts(df):
    # Make a copy of the dataframe to avoid modifying the original
    df = df.copy()
    
    # If there's no English Name column but there's a Name column
    if 'English Name' not in df.columns and 'Name' in df.columns:
        df['English Name'] = df['Name']
        df['Hebrew Name'] = df['Name']
    
    # If there's an English Name but no Hebrew Name
    if 'Hebrew Name' not in df.columns:
        df['Hebrew Name'] = df['English Name']
    
    # Process each row
    for idx, row in df.iterrows():
        # Get the name from either English Name or Name column
        name = row.get('English Name', row.get('Name', ''))
        hebrew_name = row.get('Hebrew Name', name)
        
        if pd.isna(name) or name == '':
            # If name is empty, try to get it from Hebrew Name
            name = hebrew_name
        
        if is_hebrew(str(name)):
            # If the name is in Hebrew, translate it to English and keep Hebrew
            english_name = translate_text(str(name))
            df.at[idx, 'English Name'] = english_name
            df.at[idx, 'Hebrew Name'] = name
        else:
            # If the name is in English, keep both the same
            df.at[idx, 'English Name'] = name
            if pd.isna(df.at[idx, 'Hebrew Name']):
                df.at[idx, 'Hebrew Name'] = name
    
    # Ensure all required columns exist with proper data
    if 'Email' not in df.columns:
        df['Email'] = ''
    if 'Phone' not in df.columns:
        df['Phone'] = ''
    if 'Google Contact ID' not in df.columns:
        df['Google Contact ID'] = ''
    
    # Select and order the required columns
    required_columns = ['English Name', 'Hebrew Name', 'Email', 'Phone', 'Google Contact ID']
    return df[required_columns]

if __name__ == "__main__":
    # Test the function
    test_df = pd.DataFrame({
        'Name': ['שלום עולם', 'John Doe', 'דוד כהן'],
        'Email': ['test@test.com', 'john@example.com', 'david@test.com'],
        'Phone': ['123-456', '789-012', '345-678']
    })
    result = process_contacts(test_df)
    print(result)