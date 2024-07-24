# How to use queries
## Step One: Setup Environment
This file contains 2 different jupyter notebooks. <br>
To use these notebooks, first set up an environment. If you already have a python environment, you can skip this step. Otherwise, download miniconda from
the following link: <br>
https://docs.conda.io/projects/miniconda/en/latest/ <br>
Create a new conda environment and open it with the following commands (press [y] when prompted): <br>
```conda create --name neo4j``` <br>
Next, use this command to activate the environment: <br>
```conda activate neo4j``` <br>
Finally, install pip with the following command (press [y] when prompted): <br>
```conda install pip```

## Step Two: Install Packages
Once your conda environment is activated, use the following commands to install the necessary packages: <br>
```pip install neo4j``` <br>
```pip install requests``` <br>
```pip install jupyter ``` <br>

## Step Three: Use Jupyter Notebook
You can now open the jupyter notebook and use the queries. adding_authors allows you to add authors from OpenAlex to a Neo4j graph, while openalex_author_query allows you to query OpenAlex author information.
