"""
main.py: The main entry point for database uploads.
By default runs the complete_download_and_upload script from SlurmJob.py
Takes in all the necessary arguments on the command line in the following order
slurm_account = account for sbatch files
environment_location = location of python environment for cluster
nodes = how many nodes per job
tasks_per_node = how many tasks per node
mem_per_cpu = memory per cpu
duration = duration of each job
URL = url of neo4j database to connect to
USERNAME = username for neo4j database
PASSWORD = password for neo4j database
DATABASE = which neo4j database to update
from_empty = True if injecting info into an empty database, false if updating a previously-existing database
"""
__author__ = "Keller Smith"
__last_edited__ = "10-31-23"
__license__ = "https://opensource.org/license/mit/"

import os
from SlurmJob import SlurmJob
from File import File
from os.path import exists
import json
import datetime
import sys

"""
Main file. Use to create and submit jobs.
"""
arguments = sys.argv
slurm_account = arguments[1]
environment_location = arguments[2]
nodes = arguments[3]
tasks_per_node = arguments[4]
mem_per_cpu = arguments[5]
duration = arguments[6]
URL = arguments[7]
USERNAME = arguments[8]
PASSWORD = arguments[9]
DATABASE = arguments[10]
from_empty = arguments[11]
job_submission = SlurmJob(slurm_account, environment_location, nodes, tasks_per_node, mem_per_cpu, duration)
job_submission.complete_download_and_upload(URL, USERNAME, PASSWORD, DATABASE, from_empty)
