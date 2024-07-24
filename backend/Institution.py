"""
Institution.py: Contains the Institution class, which contains methods
relating to OpenAlex institutions.
"""
__author__ = "Keller Smith"
__last_edited__ = "10-18-23"
__license__ = "https://opensource.org/license/mit/"
import json
from File import File
from DataBase import DataBase
import sys

class Institution:
    """
    A class to hold various methods relating to institutions

    Imports
    -------
    Uses the File and Database classes from this project
    json
    sys
    
    Methods
    -------
    remove_duplicate_institutions():
        Removes duplicate institutions and creates a new json file without the duplicates
    upload_institutions(url, username, password, database):
        Uploads institutions within the json file
    run_medthod(arguments):
        Used to run any method in the class to easily access methods in an sbatch file
    """
    def remove_duplicate_institutions():
        """
        Removes duplicate institutions and creates a new json file without the duplicates

        Requirements
        ------------
        openalex_institution_dump.json

        Parameters
        ----------
        None

        Returns
        -------
        None

        """
        with open('openalex_institution_dump.json') as i:
            unique_ids = set()
            institutions = json.load(i)['results']
            new_institutions = []
            for institution in institutions:
                if not(institution['id'] in unique_ids):
                    unique_ids.add(institution['id'])
                    new_institutions.append(institution)
        File.create_json_file('openalex_institution_dump_edit.json', new_institutions)
    def upload_institutions(url, username, password, database):
        """
         Uploads institutions within the json file

        Requirements
        ------------
        openalex_institution_dump_edit.json

        Parameters
        ----------
        url: str
            URL of the database to connect and upload the nodes to
        username: str
            Username of Neo4j user
        password: str
            Password of Neo4j user
        database: str
            Specific database to upload to

        Returns
        -------
        None
        """
        database_connection = DataBase(url, username, password, database)
        with open('openalex_institution_dump_edit.json') as a:
            data = json.load(a)['results']
        command = """  
            WITH $data AS batch
            UNWIND batch AS node
            CREATE (a: Institution {id: node.id})
            SET a.name = node.display_name, a.ror = node.ror, a.country_code = node.country_code, a.type = node.type
        """
        database_connection.run_command(command, additional_data=data)
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
    Institution.run_method(sys.argv[1:])
if __name__ == "__main__":
    main()