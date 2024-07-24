"""
Author.py: Contains the author class, which contains various
methods relating to authors.
"""
__author__ = "Keller Smith"
__last_edited__ = "10-18-23"
__license__ = "https://opensource.org/license/mit/"
from File import File
from DataBase import DataBase
import json
import sys
class Author:
    """
    A class to hold various methods relating to authors.

    Imports
    -------
    Uses the File and Database classes from this project
    json
    sys
    
    Methods
    -------
    remove_duplicate_authors_and_collabs():
        Removes any duplicated authors and collaborators from the author and collab json files
        Merges authors which appear in both files so that only one node will be created
    upload_current_gt_authors(url, username, password, database):
        Uploads authors within the author json file
    upload_collabs_parallel(batch_number, url, username, password, database, batch_size=8000):
        Uploads collabs within the collab json file, set up to make use of a slurm array job
    get_max_batch_number(batch_size=8000):
        Prints the length of the array used when uploading the collabs in parallel
        Use to determing correct size of array for an array job
        Prints the result to output file
    test_no_duplicates_in_author_and_collab():
        A test to determine if there are any authors which appear both as an author and a collab,
        meaning they were not propery merged
        Prints the result to output file
    check_no_lost_collabs():
        A test to determine if every collab in the original collab json is also in the edited version
        Prints the result to output file
    run_method(arguments):
        Used to run any method in the class to easily access methods in an sbatch file
    """
    def remove_duplicate_authors_and_collabs():
        """
        Removes any duplicated authors and collaborators from the author and collab json files
        Merges authors which appear in both files so that only one node will be created
        When merging nodes, combines the past institution lists to capture all institutions an author
        worked at
        Does not include GT in a current GT author's past institutions, as this will be represented
        through a WORKING_AT relationship

        Requirements
        ------------
        openalex_author_dump.json
        openalex_collab_dump.json
        openalex_work_dump.json
        openalex_institution_dump.json

        Parameters
        ----------
        None

        Returns
        -------
        None

        """
        if not (File.check_files(True, False)):
            raise FileNotFoundError("You must download all 4 openalex files.")
        with open('openalex_author_dump.json') as a, open('openalex_collab_dump.json') as c:
            gt_authors = json.load(a)['results']
            collaborators = json.load(c)['results']
        unique_ids = {}
        new_collaborators = []
        for a in collaborators:
            if not (a['id'] in unique_ids):
                unique_ids[a['id']] = a['past_institutions']
                new_collaborators.append(a)
            else:
                for inst in a['past_institutions']:
                    if inst not in unique_ids[a['id']]:
                        unique_ids[a['id']].append(inst)
        for a in new_collaborators:
            a['past_institutions'] = unique_ids[a['id']]

        final_collaborators = []
        institutes = {}
        for author in gt_authors:
            institutes[author['id']] = []
        for collab in new_collaborators:
            id = collab['id']
            if id in institutes:
                for k in collab['past_institutions']:
                    if k not in institutes[id] and not (k == 'https://openalex.org/I130701444'):
                        institutes[id].append(k)
            else:
                final_collaborators.append(collab)
        for author in gt_authors:
            author['past_institutions'] = institutes[author['id']]
        File.create_json_file('openalex_author_dump_edit.json', gt_authors)
        File.create_json_file('openalex_collab_dump_edit.json', final_collaborators)
    def upload_current_gt_authors(url, username, password, database):
        """
        Uploads authors within the author json file
        Gives all current gt authors both the Author and GT label
        Creates a WORKING_AT relationship with GT

        Requirements
        ------------
        openalex_author_dump_edit.json

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
        with open('openalex_author_dump_edit.json') as a:
            data = json.load(a)['results']
        command = """
            WITH $data AS batch
            UNWIND batch AS node
            CREATE (a:Author:GT {id: node.id})
            SET a.name = node.display_name, a.cited_by_count = node.cited_by_count, a.created_date = node.created_date, a.display_name_alternatives = node.display_name_alternatives, a.orcid = node.orcid, a.updated_date = node.updated_date, a.works_api_url = node.works_api_url, a.works_count = node.works_count
        """
        database_connection.run_command(command, additional_data=data)
        command = """
            MATCH (a:GT)
            MATCH (b:Institution {id: "https://openalex.org/I130701444"})
            MERGE (a) -[:WORKING_AT]- (b)
        """
        database_connection.run_command(command)
    def upload_collabs_parallel(batch_number, url, username, password, database, batch_size=8000):
        """
        Uploads collabs within the collab json file, set up to make use of a slurm array job
        Adds the WORKED_AT relationship with past institutions

        Requirements
        ------------
        openalex_collab_dump_edit.json

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
        database_connection = DataBase(url, username, password, database)
        file = File()
        json_chunks = file.chunk_json('openalex_collab_dump_edit.json', batch_size) 
        if batch_number < len(json_chunks):
            data = json_chunks[batch_number]
            command = """
                WITH $data AS batch
                UNWIND batch AS node
                CREATE (a: Author {id: node.id})
                SET a.name = node.display_name, a.orcid = node.orcid
                WITH a, node
                UNWIND node.past_institutions AS past
                MATCH (b: Institution {id: past.id}) 
                CREATE (a) -[:WORKED_AT]-> (b)
            """
            database_connection.run_command(command, additional_data=data)
    def get_max_batch_number(batch_size=8000):
        """
        Prints the length of the array used when uploading the collabs in parallel
        Use to determine correct size of array for an array job
        Prints the result to output file

        Requirements
        ------------
        openalex_collab_dump_edit.json

        Parameters
        ----------
        batch_size: int (optional)
            Represents the size of each batch, the number of nodes to be uploaded in each sub-job
            If the size is too large, Neo4j will run out of memory and the upload will not complete

        Returns
        -------
        None
        """
        json_chunks = File.chunk_json('openalex_collab_dump_edit.json', batch_size)
        print(len(json_chunks))
    def test_no_duplicates_in_author_and_collab():
        """
        A test to determine if there are any authors which appear both as an author and a collab,
        meaning they were not propery merged
        Prints the result to output file

        Requirements
        ------------
        openalex_author_dump_edit.json
        openalex_collab_dump_edit.json

        Parameters
        ----------
        None

        Returns
        -------
        Returns true if successful, false otherwise.
        """
        no_dups = True
        with open('openalex_author_dump_edit.json') as a, open('openalex_collab_dump_edit.json') as c:
            authors = json.load(a)['results']
            collabs = json.load(c)['results']
        author_ids = set()
        for a in authors:
            author_ids.add(a['id'])
        for c in collabs:
            if c['id'] in author_ids:
                no_dups = False
        if no_dups:
            print("There are no duplicates across authors and collaborators.")
            return True
        else:
            print("There are duplicates across authors and collaborators.")
            return False
    def check_no_lost_collabs():
        """
        A test to determine if every collab in the original collab json is also in the edited version
        Prints the result to output file

        Requirements
        ------------
        openalex_collab_dump.json
        openalex_collab_dump_edit.json

        Parameters
        ----------
        None

        Returns
        -------
        Returns true if successful, false otherwise.
        """
        contains_all = True
        with open('openalex_collab_dump.json') as f, open('openalex_collab_dump_edit.json') as c, open('openalex_author_dump_edit.json') as a:
            original_collabs = json.load(f)['results']
            edit_collabs = json.load(c)['results']
            edit_authors = json.load(a)['results']
        in_edit = set()
        for a in edit_collabs:
            in_edit.add(a['id'])
        for a in edit_authors:
            in_edit.add(a['id'])
        for a in original_collabs:
            if not (a['id'] in in_edit):
                contains_all = False
        if contains_all:
            print("All collabs are in edited file")
            return True
        else:
            print("Some collabs missing")
            return False
    def check_authors_complete():
        """
        Combines method for a complete test on the author and collaboration files.
        Error if either of these 2 tests fail.

        Requirements
        ------------
        openalex_collab_dump.json
        openalex_collab_dump_edit.json
        openalex_author_dump.json
        openalex_author_dump_edit.json

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        if not Author.test_no_duplicates_in_author_and_collab():
            raise ValueError("There are duplicates across the author and collab files. Check OpenAlex for changes.")
        if not Author.check_no_lost_collabs():
            raise ValueError("There were lost authors across the author and collab files. Check OpenAlex for changes.")
    def create_collaborated_relationship(url, username, password, database):
        """
        Creates the COLLABORATED_WITH relationship in the database

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
            MATCH (a:GT) -[:AUTHORED]- (:Work) -[:AUTHORED]- (:Author) -[:WORKED_AT]- (b:Institution) where b.name <> "Georgia Institute of Technology"
            MERGE (a) -[:COLLABORATED_WITH]-> (b)    
        """
        database_connection.run_command(command)
        command = """
            MATCH (a:Author) -[:WORKED_AT]- (b:Institution {name: "Georgia Institute of Technology"})
            WITH a, b
            MATCH (a) -[:AUTHORED]- (:Work) -[:AUTHORED]- (:Author) -[:WORKED_AT]- (c:Institution) where c.name <> "Georgia Institute of Technology"
            MERGE (a) -[:COLLABORATED_WITH]-> (c)   
        """
        database_connection.run_command(command)
    def compare_counts_in_db(url, username, password, database, equal=True):
        """
        Compares the counts in the database to the counts in the file about to be uploaded

        Requirements
        ------------
        openalex_collab_dump_edit.json
        openalex_author_dump_edit.json
        openalex_institution_dump_edit.json
        openalex_work_dump_edit.json

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
        equal: str
            If this is "True" then the script checks if the database numbers are equal to the file numbers. This is used after an upload to ensure all
            data was uploaded.
            If this is "False" then the script checks if there was a large change in the numbers. This is used before an update to ensure there was not a large
            change in OpenAlex data which requires human intervention. The current threshold id 10%.

        Returns
        -------
        None
        """
        database_connection = DataBase(url, username, password, database)
        command = """
            MATCH (a:GT)
            RETURN count(a) as NumberOfAuthors;
        """
        return_val = database_connection.run_command_with_return(command)
        count_graph_author = return_val['NumberOfAuthors']
        print(count_graph_author)
        count_file_author = File.check_num_in_json('openalex_author_dump_edit.json')
        if equal == "True":
            if not (count_graph_author == count_file_author):
                raise ConnectionError("Some author data was not properly uploaded to Neo4j")
        else:
            min = count_graph_author * 0.9
            max = count_graph_author * 1.1
            if not (min <= count_file_author <= max):
                raise ConnectionError("The authors in the database have changed by more than 10%. Please check the numbers")

        command = """
            MATCH (a:Institution)
            RETURN count(a) as NumberOfInstitutions;
        """
        return_val = database_connection.run_command_with_return(command)
        count_graph_institution = return_val['NumberOfInstitutions']
        count_file_institution = File.check_num_in_json('openalex_institution_dump_edit.json')
        print(count_graph_institution)
        if equal:
            if not (count_graph_institution == count_file_institution):
                raise ConnectionError("Some institution data was not properly uploaded to Neo4j")
        else:
            min = count_graph_institution * 0.9
            max = count_graph_institution * 1.1
            if not (min <= count_file_institution <= max):
                raise ConnectionError("The institutions in the database have changed by more than 10%. Please check the numbers")

        command = """
            MATCH (a:Work)
            RETURN count(a) as NumberOfWorks;
        """
        return_val = database_connection.run_command_with_return(command)
        count_graph_work = return_val['NumberOfWorks']
        count_file_work = File.check_num_in_json('openalex_work_dump_edit.json')
        print(count_graph_work)
        if equal:
            if not (count_graph_work == count_file_work):
                raise ConnectionError("Some work data was not properly uploaded to Neo4j")
        else:
            min = count_graph_work * 0.9
            max = count_graph_work * 1.1
            if not (min <= count_file_work <= max):
                raise ConnectionError("The works in the database have changed by more than 10%. Please check the numbers")
            
        command = """
            MATCH (a:Author)
            RETURN count(a) as NumberOfAuthors;
        """
        return_val = database_connection.run_command_with_return(command)
        count_graph_collab = return_val['NumberOfAuthors']
        count_file_collab = File.check_num_in_json('openalex_collab_dump_edit.json')
        count_file_collab = count_file_collab + count_file_author
        print(count_graph_collab)
        if equal:
            if not (count_graph_collab == count_file_collab):
                raise ConnectionError("Some collab data was not properly uploaded to Neo4j")
            print("The Database Was Successfully Updated without any errors.")
        else:
            min = count_graph_collab * 0.9
            max = count_graph_collab * 1.1
            if not (min <= count_file_collab <= max):
                raise ConnectionError("The works in the database have changed by more than 10%. Please check the numbers")
            print("Database is good to be updated")
            command = """
                MATCH (a) DETACH DELETE a
            """
            database_connection.run_command(command)
            print("Database is cleared")
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
            ex. ['Author.check_no_lost_collabs()']

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
    Author.run_method(sys.argv[1:])
if __name__ == "__main__":
    main()
