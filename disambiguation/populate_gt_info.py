"""
populate_gt_info.py: For the authors with matching ORCID IDS from both the GT ORCID export and database
Updates the database with their GT data, including GTID, email, etc.

Requires the file ORCID_EXPORT_6_14_2023.csv
"""
__author__ = "Keller Smith"
__last_edited__ = "10-18-23"
__license__ = "https://opensource.org/license/mit/"

import csv
from neo4j import GraphDatabase
URL = 'neo4j+ssc://neo4j-dev.pace.gatech.edu:7687'
USERNAME = ''
PASSWORD = ''
DATABASE = 'vip-vxg-summer2023'

first = True
gt = '{name: "Georgia Institute of Technology"}'
driver = GraphDatabase.driver(URL, auth=(USERNAME, PASSWORD))
with open("ORCID_EXPORT_6_14_2023.csv") as f:
    reader = csv.reader(f, delimiter=',')
    for author in reader:
        if not first:
            gtid = author[0]
            gtusername = author[1]
            email = author[3]
            orcid = author[4]
            orcid_as_url = "https://orcid.org/" + orcid
            print(gtid)
            command = f"""
                match (a:Author) where a.orcid = '{orcid_as_url}'
                set a.gtid = {gtid}
                set a.gtusername = '{gtusername}]'
                set a.email = '{email}'
                set a:GT
                with a
                match (b:Institution {gt})
                merge (a) -[:WORKING_AT]- (b)
                with a, b
                match (a) -[r:WORKED_AT]- (b)
                delete r
                return a
                """
            with driver.session(database=DATABASE) as session:
                    session.run(command)
        else:
             first = False
