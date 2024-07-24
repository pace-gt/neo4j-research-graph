"""
author_doi.py: The purpose of this file is to retrieve authors and their
corresponding dois from both OpenAlex and SCOPUS.

get_open_alex_dois():
    Creates a csv file, including all the dois for GT authors who do not already have a 
    ORCID ID in the database.

get_scopus_dois():
    Using the GT author CSV from SCOPUS, downloads all DOIS for the authors from SCOPUS and adds them to the CSV
    Very intensive, to be updated for efficiency in the future.
"""
__author__ = "Keller Smith, Henry Chen"
__last_edited__ = "10-18-23"
__license__ = "https://opensource.org/license/mit/"

from neo4j import GraphDatabase
import json
import requests
import pandas as pd


class Neo4jConnection:
    """
    A class to connect to a Neo4j database
    Once an instance is created, the query() method can be used to make queries.
    """
    # Constructor to initialize the Neo4jConnection object with URI, username, and password.
    def __init__(self, uri, user, pwd):
        self.__uri = uri  # Store the provided URI for the Neo4j database.
        self.__user = user  # Store the username for authentication.
        self.__pwd = pwd  # Store the password for authentication.
        self.__driver = None  # Initialize the Neo4j driver as None.
        try:
            # Attempt to create a connection to the Neo4j database using the provided credentials.
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
        except Exception as e:
            print("Failed to create the driver:", e)
    
    # Method to close the Neo4j driver and free up any associated resources.
    def close(self):
        if self.__driver is not None:
            self.__driver.close()
    
    # Method to execute a query on the Neo4j database and return the results.
    # The "db" parameter is optional and used to specify the database in the connection (defaults to "vip-vxg-summer2023").
    def query(self, query, parameters=None, db="vip-vxg-summer2023"):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try:
            # Open a session to the specified database (default "vip-vxg-summer2023") or the default database if None is provided.
            session = self.__driver.session(database=db) if db is not None else self.__driver.session() 
            # Execute the provided query with optional parameters and store the response as a list of results.
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        finally:
            # Close the session to release any acquired resources.
            if session is not None:
                session.close()
        return response
def get_open_alex_dois():
    """
        Downloads all GT authors from OpenAlex who do not already have an ORCID ID in the database
        Puts them in a CSV file
    """
    conn = Neo4jConnection(uri = "neo4j+ssc://neo4j-dev.pace.gatech.edu:7687", 
                        user="hchen769",              
                        pwd="Henry0115!")
    results = conn.query("""
                        OPTIONAL MATCH (a:GT) where a.orcid is null
                        with a
                        match (a) -[:AUTHORED]- (b:Work) where b.doi is not null
                        return a.name as Name, a.id as ID, b.doi as DOI
    """ )
    columns = ["Name", "ID", "DOI"]
    data = [[record[col] for col in columns] for record in results]
    df = pd.DataFrame(data, columns=columns)
    df.shape[0]
    file_path = "OA_DOI.csv"
    df.to_csv(file_path)
    conn.close()
def get_scopus_dois():
    """
    Using the GT author CSV from SCOPUS, downloads all DOIS for the authors from SCOPUS and adds them to the CSV
    Very intensive, to be updated for efficiency in the future.
    """
    file_name = "GT_authors_scopus.csv"
    scopus_df = pd.read_csv(file_name)
    scopus_df.drop("Unnamed: 5", axis=1, inplace=True)
    scopus_df.drop("Subject Area", axis=1, inplace=True)
    scopus_df.drop("Number of Documents", axis=1, inplace=True)
    # scopus_df['Subject Area'] = scopus_df['Subject Area'].str.strip().str.split('\n')
    
    data1 = []
    for i in range(0, 22702):
        auth_id = scopus_df['Auth-ID'][i]
        orc_id = scopus_df['Orc_ID'][i]
        name = scopus_df['Author Name'][i]
        auth_row = [i, name, auth_id, orc_id]
        url = f"https://api.elsevier.com/content/search/scopus?query=AU-ID({auth_id})&apiKey=709251e2a05834d5dfd51d1c9a57f4a0"
        response = requests.get(url)
        if response.status_code != 200:
            print("Error:", response.status_code, response.text, response.headers)
            while response.status_code != 200:
                response = requests.get(url)
        data = response.json()
        works = data.get('search-results', {}).get('entry', [])
        DOIs = []
        for work in works:
            DOIs.append(work.get('prism:doi'))
            auth_row.append(DOIs)
            data1.append(auth_row)

    df = pd.DataFrame(data1, columns=['index', 'Author Name', 'Auth-ID', 'Orc-ID', 'DOIs'])
    Scopus_DOI_explode = df.explode('DOIs')
    print(Scopus_DOI_explode)
    file_path = "SCOPUS_DOI.csv"
    Scopus_DOI_explode.to_csv(file_path)

get_open_alex_dois()
get_scopus_dois()