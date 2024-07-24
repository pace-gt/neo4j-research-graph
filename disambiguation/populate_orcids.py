"""
populate_orcids.py: Merges the 2 different DOI csvs to determine more author ORCIDS.
If an OA author and a SCOPUS author share a DOI and last name, it can be determined that they
are the same author.
Requires Scopus_DOI_explode.csv and OA_DOI.csv
"""
__author__ = "Keller Smith, Henry Chen"
__last_edited__ = "10-18-23"
__license__ = "https://opensource.org/license/mit/"

import pandas as pd
import re

SCO_DOI = pd.read_csv("Scopus_DOI_explode.csv")
OA_DOI = pd.read_csv("OA_DOI.csv")
OA_DOI['DOI'] = OA_DOI['DOI'].str.replace("https://doi.org/", "", regex=False)
OA_DOI = OA_DOI.drop_duplicates(subset=['DOI', 'ID'], keep='first')

SCO_DOI.rename(columns={'DOIs': 'DOI'}, inplace=True)
SCO_DOI = SCO_DOI.drop_duplicates(subset=['DOI', 'Auth-ID'], keep='first')

final_merge = pd.merge(OA_DOI, SCO_DOI, on='DOI')
final_merge = final_merge[final_merge['DOI'].notnull()]
final_merge.head(100)

reg = re.compile(r'[a-zA-Z]')
# Defining a function to split OpenAlex names into first and last names
def split_name_OpenAlex(name):
    # Using regex to match and check if the name is in English
    if ' ' in name and reg.match(name):
        # Checking for suffixes like 'III' or 'II' and removing them if they exist
        if name.rsplit(' ', 1)[-1] == 'III' or name.rsplit(' ', 1)[-1] == 'II':
            name2 = name.rsplit(' ', 1)[0]
            return name2.rsplit(' ', 1)[0], name2.rsplit(' ', 1)[1]
        # Handling names with a space: first name is before the space, last name is after.
        return name.rsplit(' ', 1)[0], name.rsplit(' ', 1)[1]
    else:
        # Handling names without a space: first name is all but the last character, last name is the last character.
        if ' ' in name:
            return name.split(' ', 1)[1], name.split(' ', 1)[0]
        else:
            return name[1:], name[0]
def split_name_Scopus(name):
    # Using regex to match and check if the name is in English
    if ',' in name and reg.match(name):
        # Handling names with a comma: first name is after the comma, last name is before.
        return name.split(',')[1], name.split(',')[0]
    else:
        # Handling names without a space: first name is all but the last character, last name is the last character.
        if ' ' in name:
            return name.split(' ', 1)[1], name.split(' ', 1)[0]
        else:
            return name[1:], name[0]
# Applying the split_name functions to create 'First_Name' and 'Last_Name' columns for OpenAlex and Scopus names
final_merge['First_Name_OpenAlex'], final_merge['Last_Name_OpenAlex'] = zip(*final_merge['Name'].apply(split_name_OpenAlex))
final_merge['First_Name_Scopus'], final_merge['Last_Name_Scopus'] = zip(*final_merge['Author Name'].apply(split_name_Scopus))

# Displaying the DataFrame with added 'First_Name' and 'Last_Name' columns
final_merge

# Saving the modified DataFrame to a CSV file named "DOI_Match.csv"
final_merge.to_csv("DOI_Match.csv", index=False)

# Using boolean indexing to filter rows where the values in 'Last_Name_OpenAlex' are equal to the values in 'Last_Name_Scopus'
rows_matching_LastNames = final_merge[final_merge['Last_Name_OpenAlex'] == final_merge['Last_Name_Scopus']]

# Displaying the filtered DataFrame with matching last names
rows_matching_LastNames

# Remove all rows without an ORCID
rows_matching_LastNames = rows_matching_LastNames[
    (rows_matching_LastNames['Orc_ID'].notna()) & (rows_matching_LastNames['Orc_ID'] != '')
]
# Print number of new orcid IDs
print("Number of new, unique ORCID IDs in CSV: ")
print(rows_matching_LastNames['Orc_ID'].nunique())
# Saving the filtered DataFrame with matching last names to a CSV file named "DOI_LastName_Match.csv"
rows_matching_LastNames.to_csv("DOI_LastName_Match.csv", index=False)