"""
Work.py: Contains the Work class, which contains methods related to
the OpenAlex Work entity.
"""
__author__ = "Keller Smith"
__last_edited__ = "10-18-23"
__license__ = "https://opensource.org/license/mit/"

from DataBase import DataBase
from File import File
import json
import sys

class Work:
    """
    A class to hold various methods relating to works

    Imports
    -------
    Uses the File and Database classes from this project
    json
    sys
    
    Methods
    -------
    remove_duplicate_works():
        Removes duplicate works and creates a new json file without the duplicates
    upload_works_parallel(batch_number, url, username, password, database, batch_size=8000):
        Uploads works within the work json file, set up to make use of a slurm array job
    get_max_batch_number(batch_size=8000):
        Prints the length of the array used when uploading the works in parallel
        Use to determing correct size of array for an array job
        Prints the result to output file
    create_authored_relationship(url, username, password, database):
        Creates the authored relationship within the database
    run_medthod(arguments):
        Used to run any method in the class to easily access methods in an sbatch file
    """
    def remove_duplicate_works():
        """
        Removes duplicate works and creates a new json file without the duplicates
        
        Requirements
        ------------
        openalex_work_dump.json

        Parameters
        ----------
        None

        Returns
        -------
        None

        """
        with open('openalex_work_dump.json') as w:
            unique_ids = set()
            works = json.load(w)['results']
            new_works = []
            for work in works:
                if not(work['id'] in unique_ids):
                    unique_ids.add(work['id'])
                    work['author_ids'] = []
                    for a in work['authorships']:
                        work['author_ids'].append(a['author']['id'])
                    new_works.append(work)
        File.create_json_file('openalex_work_dump_edit.json', new_works)
    def upload_works_parallel(batch_number, url, username, password, database, batch_size=8000):
        """
        Uploads works within the work json file, set up to make use of a slurm array job

        Requirements
        ------------
        openalex_work_dump_edit.json

        Parameters
        ----------
        batch_number: int
            Represents which batch to upload, based on batch_size
            with the default number for batch_size (8000): 
            A batch number of 0 uploads the first 8000, 1 the next 8000, etc 
        url: str
            URL of the database to connect and upload the nodes to
        username: str
            Username of Neo4j user
        password: str
            Password of Neo4j user
        database: str
            Specific database to upload to
        batch_size: int (optional)
            Represents the size of each batch, the number of nodes to be uploaded in each sub-job
            If the size is too large, Neo4j will run out of memory and the upload will not complete
            Default value: 8000

        Returns
        -------
        None
        """
        file = File()
        json_chunks = file.chunk_json('openalex_work_dump_edit.json', batch_size) 
        database_connection = DataBase(url, username, password, database)
        if batch_number < len(json_chunks):
            data = json_chunks[batch_number]
            command = """
                    WITH $data AS batch
                    UNWIND batch AS node
                    CREATE (a: Work {id: node.id})
                    SET a.name = node.display_name, a.cited_by_api_url = node.cited_by_api_url, a.cited_by_count = node.cited_by_count, a.corresponding_author_ids = node.corresponding_author_ids, a.corresponding_institution_ids = node.corresponding_institution_ids, a.created_date = node.created_date, a.doi = node.doi, a.is_paratext = node.is_paratext, a.is_retracted = node.is_retracted, a.language = node.language, a.publication_date = node.publication_date, a.publication_year = node.publication_year, a.referenced_works = node.referenced_works, a.related_works = node.related_works, a.title = node.title, a.type = node.type, a.updated_date = node.updated_date, a.is_oa = node.is_oa, a.license = node.license, a.url = node.url, a.version = node.version, a.author_ids = node.author_ids
                """
            database_connection.run_command(command, additional_data=data)
    def get_max_batch_number(batch_size=8000):
        """
        Prints the length of the array used when uploading the works in parallel
        Use to determine correct size of array for an array job
        Prints the result to output file

        Requirements
        ------------
        openalex_work_dump_edit.json

        Parameters
        ----------
        batch_size: int (optional)
            Represents the size of each batch, the number of nodes to be uploaded in each sub-job
            If the size is too large, Neo4j will run out of memory and the upload will not complete

        Returns
        -------
        None
        """
        json_chunks = File.chunk_json('openalex_work_dump_edit.json', batch_size)
        print(len(json_chunks))
    def create_authored_relationship(url, username, password, database):
        """
        Creates the authored relationship within the database

        Requirements
        ------------
        None

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
        command = """
            Match (a:Work)
            unwind a.author_ids as authors
            Match (b:Author {id:authors})
            Create (b) -[:AUTHORED]-> (a)       
        """
        database_connection.run_command(command)
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
    Work.run_method(sys.argv[1:])
if __name__ == "__main__":
    main()