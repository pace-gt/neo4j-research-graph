import os
import json
import sys
from os.path import exists
"""
File.py: Contains the file class, which holds various methods relating to 
file manipulation.
"""
__author__ = "Keller Smith"
__last_edited__ = "10-18-23"
__license__ = "https://opensource.org/license/mit/"
import datetime
class File:
    """
    A class to hold various methods relating to files

    Imports
    -------
    os
    json
    sys
    
    Methods
    -------
    close_file(file_name):
        Adds the necessary ending to a given json file once all data has been imported
    chunk_json(filename, chunk_size):
        Separates the json file into smaller chunks and returns them in the form of a list
    open_file(file_name):
        Adds the necessary beginning to a new json file before adding the data
    write_data_to_file(file_name, data):
        Writes each item in a given list to the given json file
    create_json_file(file_name, data):
        Combines methods to create a new json file with the given data in the correct format.
    check_num_in_json(file_name):
        Prints the number of items inside a given json file
    compare_to_edit(original_file, edit_file):
        Ensures all entries in the original json are also in the edited json
    check_for_duplicates(edit_file):
        Checks for duplicates in the given file
    get_max_batch_number_works(batch_size=8000):
        Prints length of chunked list for works json
    get_max_batch_number_collabs(batch_size=8000):
        Prints length of chunked list for the collab json
    check_files(original=True, edit=True):
        Checks to see if certain files exist within the current directory
    run_method(arguments):
        Used to run any method in the class to easily access methods in an sbatch file
    """
    def close_file(file_name):
        """
        Adds the necessary ending to a given json file once all data has been imported

        Requirements
        ------------
        None

        Parameters
        ----------
        file_name: str
            Name of file in the current directory to use

        Returns
        -------
        None
        """
        with open(file_name, 'rb+') as f:
            f.seek(-1, os.SEEK_END)
            f.truncate()
        with open(file_name, 'a') as f:
            f.write(']}')
    def chunk_json(self, file_name, chunk_size):
        """
        Separates the json file into smaller chunks and returns them in the form of a list
        Used to run methods in parallel

        Requirements
        ------------
        None

        Parameters
        ----------
        file_name: str
            Name of file in the current directory to use
        chunk_size: int
            Nummber of items in each chunk

        Returns
        -------
        list of items in the original file separated into chunks of the given size
        """
        with open(file_name) as f:
            data = json.load(f)['results']
        chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]
        return chunks
    def open_file(file_name):
        """
        Adds the necessary beginning to a new json file before adding the data

        Requirements
        ------------
        None

        Parameters
        ----------
        file_name: str
            Name of file in the current directory to use

        Returns
        -------
        None
        """
        with open(file_name, 'a') as f:
            f.write('{"results": [')
    def write_data_to_file(file_name, data):
        """
        Writes each item in a given list to the given json file

        Requirements
        ------------
        None

        Parameters
        ----------
        file_name: str
            Name of file in the current directory to use
        data:
            List of data to be written to json file

        Returns
        -------
        None
        """
        with open(file_name, 'a') as f:
            for item in data:
                f.write(json.dumps(item) + ",")
    def create_json_file(file_name, data):
        """
        Combines methods to create a new json file with the given data in the correct format.

        Requirements
        ------------
        None

        Parameters
        ----------
        file_name: str
            Name of file in the current directory to use
        data:
            List of data to be written to json file

        Returns
        -------
        None
        """
        File.open_file(file_name)
        File.write_data_to_file(file_name, data)
        File.close_file(file_name)
    def check_num_in_json(file_name):
        """
        Prints the number of items inside a given json file
        Prints result to output file

        Requirements
        ------------
        None

        Parameters
        ----------
        file_name: str
            Name of file in the current directory to use

        Returns
        -------
        The count of items
        """
        count = 0
        with open(file_name) as f:
            data = json.load(f)['results']
        for k in data:
            count += 1
        print(str(file_name) + ": " + str(count))
        return count
    def compare_to_edit(original_file, edit_file):
        """
        Ensures all entries in the original json are also in the edited json
        Prints result to output file

        Requirements
        ------------
        None

        Parameters
        ----------
        original_file: str
            Name of original file in current directory to use
        edit_file: str
            Name of edited file in current directory to use

        Returns
        -------
        Returns true if successful, and false otherwise
        """
        with open(original_file) as a, open(edit_file) as b:
            contains_all = True
            original_data = json.load(a)['results']
            edit_data = json.load(b)['results']
            in_edit = set()
            for a in edit_data:
                in_edit.add(a['id'])
            for a in original_data:
                if not(a['id'] in in_edit):
                    contains_all = False
                    print("MISSING: " + str(a['id']))
            if contains_all:
                print("All objects in original file are in the edited file for " + str(edit_file))
                return True
            else:
                print(str(edit_file) + " is missing some data.")
                return False
    def check_for_duplicates(edit_file):
        """
        Checks for duplicates in the given file
        Prints result to output file

        Requirements
        ------------
        None

        Parameters
        ----------
        file_name: str
            Name of file in the current directory to use

        Returns
        -------
        Returns true if there are no duplicates, and false otherwise.
        """
        duplicates = False
        with open(edit_file) as f:
            data = json.load(f)['results']
        in_edit = set()
        for entry in data:
            if entry['id'] in in_edit:
                duplicates = True
            in_edit.add(entry['id'])
        if duplicates:
            print("There are duplicates in " + str(edit_file))
            return False
        else:
            print("There are no duplicates in " + str(edit_file))
            return True
    def get_max_batch_number_works(self, batch_size=8000):
        """
        Prints length of chunked list for works json
        Use to determine correct size of array for an array job
        Currently returns 29, as this is large enough for a batch number of 8,000

        Requirements
        ------------
        openalex_work_dump_edit.json

        Parameters
        ----------
        batch_size: int, optional
            Number of entries to be in each chunk
            Default value: 8000

        Returns
        -------
        Length of the chunked json
        """
        #json_chunks = self.chunk_json('openalex_work_dump_edit.json', batch_size)
        #return len(json_chunks)
        return 29
    def get_max_batch_number_collabs(self, batch_size=8000):
        """
        Prints length of chunked list for collabs json
        Use to determine correct size of array for an array job
        Currently returns 39, as this is large enough for a batch size of 8000

        Requirements
        ------------
        openalex_collab_dump_edit.json

        Parameters
        ----------
        batch_size: int, optional
            Number of entries to be in each chunk
            Default value: 8000

        Returns
        -------
        Length of the chunked json
        """
        #json_chunks = self.chunk_json('openalex_collab_dump_edit.json', batch_size)
        #return len(json_chunks)
        return 39
    def check_files(original=True, edit=True):
        """
        Checks to see if certain files exist within the current directory

        Requirements
        ------------
        None

        Parameters
        ----------
        original: boolean, optional
            If true, then the method will make sure all four original json files are in the current
            directory
            Default value: True
        edit: boolean, optional
            If true, then the method will make sure all four edited json files are in the current
            directory
            Default value: True

        Returns
        -------
        boolean representing if the specific files were located or not
        """
        if original == False and edit == False:
            return True
        elif original == True and edit == False:
            return exists('openalex_work_dump.json') and exists('openalex_author_dump.json') and exists('openalex_institution_dump.json') and exists('openalex_collab_dump.json')
        elif original == False and edit == True:
            return exists('openalex_work_dump_edit.json') and exists('openalex_author_dump_edit.json') and exists('openalex_institution_dump_edit.json') and exists('openalex_collab_dump_edit.json')
        else:
            return exists('openalex_work_dump_edit.json') and exists('openalex_author_dump_edit.json') and exists('openalex_institution_dump_edit.json') and exists('openalex_collab_dump_edit.json') and exists('openalex_work_dump.json') and exists('openalex_author_dump.json') and exists('openalex_institution_dump.json') and exists('openalex_collab_dump.json')
    def test_download():
        """
        Runs tests on the download to ensure there were no issues
        Checks if all files were downloaded, and theat they all are able to be converted to JSON format.
        Error if any of the above are not true.

        Requirements
        ------------
        None

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        bool = File.check_files(True, False)
        if not bool:
            raise FileNotFoundError("At least one necessary input file was not downloaded.")
        with open('openalex_author_dump.json') as f:
            try:
                data = json.load(f)['results']
            except:
                raise TypeError("The data in openalex_author_dump.json was unable to be converted into a dictionary. The json is most likely empty. Check OpenAlex for changes.")
        with open('openalex_collab_dump.json') as f:
            try:
                data = json.load(f)['results']
            except:
                raise TypeError("The data in openalex_collab_dump.json was unable to be converted into a dictionary. The json is most likely empty. Check OpenAlex for changes.")
        with open('openalex_institution_dump.json') as f:
            try:
                data = json.load(f)['results']
            except:
                raise TypeError("The data in openalex_institution_dump.json was unable to be converted into a dictionary. The json is most likely empty. Check OpenAlex for changes.")
        with open('openalex_work_dump.json') as f:
            try:
                data = json.load(f)['results']
            except:
                raise TypeError("The data in openalex_work_dump.json was unable to be converted into a dictionary. The json is most likely empty. Check OpenAlex for changes.")
    def check_cleaned_data():
        """
        Checks for duplicates and lost information using scripts above
        Errors if there are duplicates or lost information

        Requirements
        ------------
        All edited and original files

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        new_files = ['openalex_author_dump_edit.json', 'openalex_collab_dump_edit.json', 'openalex_institution_dump_edit.json', 'openalex_work_dump_edit.json']
        old_files = ['openalex_author_dump.json', 'openalex_collab_dump.json', 'openalex_institution_dump.json', 'openalex_work_dump.json']
        for x in range(0, 4):
            if not File.check_for_duplicates(new_files[x]):
                raise ValueError("There are duplicates in " + new_files[x] + ". Check OpenAlex for changes.")
            if not x == 1:
                if not File.compare_to_edit(old_files[x], new_files[x]):
                    raise ValueError("There is lost information in " + new_files[x] + ". Check OpenAlex for changes.")
    def rename_files():
        """
        Renames files to include a time stamp

        Requirements
        ------------
        All edited and original files

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        date = datetime.date.today()
        os.rename('openalex_work_dump.json', ('openalex_work_dump_' + str(date) + '.json'))
        os.rename('openalex_work_dump_edit.json', ('openalex_work_dump_edit_' + str(date) + '.json'))
        os.rename('openalex_author_dump.json', ('openalex_author_dump_' + str(date) + '.json'))
        os.rename('openalex_author_dump_edit.json', ('openalex_author_dump_edit_' + str(date) + '.json'))
        os.rename('openalex_institution_dump.json', ('openalex_institution_dump_' + str(date) + '.json'))
        os.rename('openalex_institution_dump_edit.json', ('openalex_institution_dump_edit_' + str(date) + '.json'))
        os.rename('openalex_collab_dump.json', ('openalex_collab_dump_' + str(date) + '.json'))
        os.rename('openalex_collab_dump_edit.json', ('openalex_collab_dump_edit_' + str(date) + '.json'))
        print("The Database was successfully updated without any errors!")
    def run_method(arguments):
        """
        Used to run any method in the class to easily access methods in an sbatch file

        Requirements
        ------------
        None

        Parameters
        ----------
        arguments: list
            List of methods to execute

        Returns
        -------
        None
        """
        for method in arguments:
            eval(method)
    

def main():
    """
        The main method
        Used to run methods with an sbatch file

        Requirements
        ------------
        None

        Parameters
        ----------
        arguments: str
            Takes arguments from the command line to run specific methods

        Returns
        -------
        None
        """
    File.run_method(sys.argv[1:])
if __name__ == "__main__":
    main()