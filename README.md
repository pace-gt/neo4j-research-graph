# Neo4j-Research Graph

# Introduction
This project makes use of Python to directly connect to a Neo4j database and upload information from the OpenAlex api. <br>
The goal is to create a database based on OpenAlex with information relevant to Georgia Tech, including past and present authors, works by these authors, their collaborators, and their collaborators' institutions. <br>
The code is setupe to be run on the PACE cluster in order to greatly reduce the amount of time it takes to complete a full dump into the Neo4j database. <br>
In order to connect to the database, the code makes use of the GraphDatabase import from Neo4j. <br>

# Getting Started
1. The docs folder contains code and design documentation.
2. neo4j-queries contains a Jupyter Notebook including simple queries to a Neo4j database.
3. backend contains python scripts to create the database.
4. disambiguation contains python scripts relating to disambiguation using SCOPUS.
5. use_cases contains a jupyter notebook which allows users to get metrics from the current database and create graph visualizations. It also contains information on how to run graph visualizations on Neo4j Bloom.

## Sample Outputs
Use cases sample outputs include the following:
![query1](https://github.gatech.edu/storage/user/59425/files/781259f7-a1df-43f5-ad35-479d944b59ec)
<img width="950" alt="query1table" src="https://github.gatech.edu/storage/user/59425/files/3533c46f-81fe-44f8-9be3-92775f47886a">
<img width="526" alt="top10_gt_authors_who_collaborate_with_outside_authors" src="images/query1.png">


# Viewing the Code Documentation
The code is documented via comments in the code, as well as through html and latex created through the use of Doxygen. If you have cloned the repository to your machine, you can click on "index.html" within the html folder to view the documentation as a webpage. To view the documentation through Github, view the PDF version created by latex in the Code Documentation folder.

# Creating Code Documentation
First, ensure you have downloaded doxygen. Use this link: <br>
https://www.doxygen.nl/manual/install.html <br>
Then, on your local machine, navigate to the Create_Database directory (where the code is). A copy of the Doxyfile is located here, and you can edit the Doxyfile to change the documentation. use the command: <br>
`doxygen Doxyfile` <br>
From here, you can view the html. Additionally, navigate to the latex folder and use type "make" into the command line to create the pdf version. The documentation will be created in the current directory, so move it to the docs directory as needed.
