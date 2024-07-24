"""
author_doi.py: The purpose of this file is to retrieve gt nodes from OA which do not have an ORCID
"""
__author__ = "Keller Smith, Henry Chen"
__last_edited__ = "11-21-23"
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
def get_open_alex():
    """
        Downloads all GT authors from OpenAlex who do not already have an ORCID ID in the database
        Puts them in a CSV file
    """
    conn = Neo4jConnection(uri = "neo4j+ssc://neo4j-dev.pace.gatech.edu:7687", 
                        user="hchen769",              
                        pwd="Henry0115!")
    results = conn.query("""
                        OPTIONAL MATCH (a:GT) where a.orcid is null
                        return a.name as Name, a.id as ID
    """ )
    columns = ["Name", "ID"]
    data = [[record[col] for col in columns] for record in results]
    df = pd.DataFrame(data, columns=columns)
    df.shape[0]
    file_path = "OA_authors_no_orcid.csv"
    df.to_csv(file_path)
    conn.close()

    df = df.drop('ID', axis=1)
    file_path = "OA_authors_no_orcid_just_name.csv"
    df.to_csv(file_path)

    

get_open_alex()
print("DONE")