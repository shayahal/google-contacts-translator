from googletrans import Translator
import re

def is_hebrew(text):
    if not isinstance(text, str) or text.strip() == '':
        return False
    hebrew_pattern = re.compile(r'[\u0590-\u05FF]')
    return bool(hebrew_pattern.search(text))

def process_contacts(contacts_data):
    """
    Process contacts and translate Hebrew names
    
    Args:
        contacts_data (list): List of dictionaries containing contact information
        
    Returns:
        list: List of processed contacts with translated names
    """
    translator = Translator()
    
    for contact in contacts_data:
        if 'Name' in contact and any('\u0590' <= c <= '\u05FF' for c in str(contact['Name'])):
            contact['Translated_Name'] = translator.translate(contact['Name'], src='he', dest='en').text
    
    return contacts_data

# Only if you want to run this directly
if __name__ == "__main__":
    # Test code here
    pass