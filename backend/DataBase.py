"""
DataBase.py: Contains the DataBase class, which allows the user
to connect and run queries on a Neo4j database.
"""
__author__ = "Keller Smith"
__last_edited__ = "10-18-23"
__license__ = "https://opensource.org/license/mit/"
from neo4j import GraphDatabase
class DataBase:
    """
    A class which represents a connection to a Neo4j database

    Imports
    -------
    GraphDatabase from neo4j

    Attributes
    ----------
    url: str
        URL of the database to establish a connection with
    username: str
        Username of Neo4j user
    password: str
        Password of Neo4j user
    database: str
        Specific database to upload to
    driver: GraphDatabase.driver
        Established connection to Neo4j based on above attributes
    
    Methods
    -------
    run_command(command, additional_data=None):
        Runs a given cypher command on the database
    """
    def __init__(self, url, username, password, database):
        """
        Constructs attributes for a DataBase object and establishes a driver

        Parameters
        ----------
        url: str
            URL of the database to connect to
        username: str
            Username of Neo4j user
        password: str
            Password of Neo4j user
        database: str
            Specific database to connect to
        """
        self.url = url
        self.username = username
        self.password = password
        self.database = database
        self.driver = GraphDatabase.driver(url, auth=(username, password))
    def run_command(self, command, additional_data=None):
        """
        Runs a given cypher command on the database

        Requirements
        ------------
        None

        Parameters
        ----------
        command: str
            Cypher command to run on the Neo4j database
        additional_data: list, optional
            Additional data to give to Neo4j
            Often a list of nodes to add
            Default value: None

        Returns
        -------
        None
        """
        if (additional_data is None):
            with self.driver.session(database=self.database) as session:
               session.run(command)
        else:
            with self.driver.session(database=self.database) as session:
                session.run(command, data=additional_data)
    def run_command_with_return(self, command, additional_data=None):
        """
        Runs a given cypher command on the database

        Requirements
        ------------
        None

        Parameters
        ----------
        command: str
            Cypher command to run on the Neo4j database
        additional_data: list, optional
            Additional data to give to Neo4j
            Often a list of nodes to add
            Default value: None

        Returns
        -------
        Return value of the database call
        """
        if (additional_data is None):
            with self.driver.session(database=self.database) as session:
               result = session.run(command)
               for k in result:
                   return k
        else:
            with self.driver.session(database=self.database) as session:
                result = session.run(command, data=additional_data)
                for k in result:
                    return k
