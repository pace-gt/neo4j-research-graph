"""
SlurmJob.py: Contains the SlurmJob class, which contains methods
to create and submit sbatch files.
"""
__author__ = "Keller Smith"
__last_edited__ = "10-30-23"
__license__ = "https://opensource.org/license/mit/"

import os
from File import File
from subprocess import Popen, PIPE
import datetime
class SlurmJob:
    """
    A class representing a slurm job

    Imports
    -------
    Uses the File class from this project
    os
    Popen, PIPE from subprocess
    
    Methods
    -------
    remove_duplicates():
        Create and submit a job to remove all duplicates from the downloaded files
    download_files():
        Create and submit a job to download all files
    upload_to_neo4j():
        Create and submit a job to upload all nodes to Neo4j
    run_tests_on_cleaning():
        Create and submit a job to run multiple tests on the cleaning portion
    print_nums_in_files()
        Create and submit a job to print the number of nodes in each file
    __create_slurm_job(file_name, job_name="neo4j"):
        Creates a job with the given characteristics
    __create_parallel_slurm_job(file_name, job_name="neo4j, array_size=1):
        Creates a parallel job with the given characteristics
    __execute_slurm_job(self, file_name, dependency=None):
        Submits a given job to the cluster
    """
    def __init__(self, account, python_env_location, num_nodes=1, cores_per_node=4, mem_per_core='7G', job_duration='3:00:00'):
        """
        Constructs attributes for a SlurmJob object
        These attributes include the information necessary to construct Slurm Jobs
        
        Parameters
        ----------
        account: str
            Account of the pace user
        python_env_location: str
            Location of python virtual environment
            If following the docs on submitting a pace job, the location will be:
            /storage/coda1/p-pace-user/0/<USERNAME>/test_installs/neo4j_venv/bin/activate
        num_nodes: str, optional
            Number of nodes to use for job
            Default value: 1
        cores_per_node: str, optional
            Number of cores per node to use for job
            Default value: 4
        mem_per_core: str, optional
            Memory to use per core
            Default value: 7G
        job_duration: str, optional
            Time allotted for job
            In format HH:MM:SS
            Default value: 3:00:00
        """
        self.account = account
        self.num_nodes = num_nodes
        self.cores_per_node = cores_per_node
        self.mem_per_core = mem_per_core
        self.job_duration = job_duration
        self.python_env_location = python_env_location + "/bin/activate"
    def complete_download_and_upload(self, url, username, password, database, fromEmpty=False):
        """
        Combines scripts to do everything at once.
        Downloads the necessary objects from OpenAlex
        Tests to make sure everything was downloaded correctly
        Removes any duplicate downloads
        Tests to make sure only duplicated information was removed
        Resets the database (if needed) and ensures change in DB numbers is not too large
        Uploads information to Neo4j
        Tests to make sure everything was correctly uploaded
        Renames download files to include a timestamp

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
        fromEmpty: boolean
            Indicates if the database is already empty or needs to be deleted first. If it needs to be deleted first,
            checks to make sure too much data has not been changed.

        Returns
        -------
        None
        """
        file = File()
        # Submit the job to download all files
        job = self.download_files()
        # Runs some tests on the unedited files
        job = self.test_download(job)
        # File cleaning
        job = self.remove_duplicates(job)
        # Run some tests on the cleaned files
        job = self.run_tests_on_cleaning(job)
        if (fromEmpty == False):
            # Check new counts and reset the database
            job = self.reset_current_db(url, username, password, database, job)
        # Upload to Neo4j
        job = self.upload_to_neo4j(url, username, password, database, job)
        # Test now that everything is in the database
        job = self.test_db_post(url, username, password, database, job)
        # Rename Files
        job = self.rename_files(job)
    def complete_upload(self, url, username, password, database, fromEmpty=False):
        """
        Same as the complete script, but only does the upload portion
        Uploads information to Neo4j
        Tests to make sure everything was correctly uploaded
        Renames download files to include a timestamp

        Requirements
        ------------
        openalex_author_dump_edit.json
        openalex_work_dump_edit.json
        openalex_collab_dump_edit.json
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
        fromEmpty: boolean
            Indicates if the database is already empty or needs to be deleted first. If it needs to be deleted first,
            checks to make sure too much data has not been changed.

        Returns
        -------
        None
        """
        file = File()
        # Upload to Neo4j
        job = self.upload_to_neo4j(url, username, password, database)
        # Test now that everything is in the database
        job = self.test_db_post(url, username, password, database, job)
        # Rename Files
        job = self.rename_files(job)
    def download_files(self, dependency=None):
        """
        Create and submit a job to download all files from OpenAlex
        Downloads 4 different JSON files from OpenAlex: Authors, Institutions, Collabs, and Works
        Uses 2 major filters to obtain information: All works with GT listed as an institution, and all
        authors with GT as last known institution
        Creates an sbatch file to submit: download_files.sbatch
        Uses Download.py

        Requirements
        ------------
        None

        Parameters
        ----------
        dependency:
            Default value: None
            If this value is not none, the slurm job will be created with a dependency to only
            run after successful completion of the dependency

        Returns
        -------
        return value of the Slurm job, used to create a dependency
        """
        self.__create_slurm_job('download_files.sbatch', "Download")
        with open('download_files.sbatch', 'a') as f:
            f.write("python Download.py")
        if dependency == None:
            ret = self.__execute_slurm_job('download_files.sbatch')
        else:
            ret = self.__execute_slurm_job('download_files.sbatch', dependency)
        print ("Submitted " + str(ret))
        return ret
    def test_download(self, dependency=None):
        """
        Checks to ensure download went correctly
        Ensures all 4 files were downloaded, and that all 4 can be converted to a JSON
        Creates an sbatch file to submit: test_download.sbatch
        Uses File.py

        Requirements
        ------------
        openalex_author_dump.json
        openalex_work_dump.json
        openalex_collab_dump.json
        openalex_institution_dump.json

        Parameters
        ----------
        dependency:
            Default value: None
            If this value is not none, the slurm job will be created with a dependency to only
            run after successful completion of the dependency

        Returns
        -------
        return value of the Slurm job, used to create a dependency
        """
        self.__create_slurm_job('test_download.sbatch', "downloadTest")
        with open('test_download.sbatch', 'a') as f:
            f.write('''python File.py "File.test_download()"''')
        if dependency == None:
            ret = self.__execute_slurm_job('test_download.sbatch')
        else:
            ret = self.__execute_slurm_job('test_download.sbatch', dependency)
        print ("Submitted " + str(ret))
        return ret
    def remove_duplicates(self, dependency=None):
        """
        Create and submit a job to remove all duplicates from the downloaded files
        When downloading from OpenAlex, we often download the same object multiple times. This 
        job removes those repeated objects.
        Creates an sbatch file to submit: clean_data.sbatch
        Uses the following methods:
        Author.remove_duplicate_authors_and_collabs()
        Institution.remove_duplicate_institutions()
        Work.remove_duplicate_works()

        Requirements
        ------------
        openalex_author_dump.json
        openalex_work_dump.json
        openalex_collab_dump.json
        openalex_institution_dump.json

        Parameters
        ----------
        dependency:
            Default value: None
            If this value is not none, the slurm job will be created with a dependency to only
            run after successful completion of the dependency

        Returns
        -------
        return value of the Slurm job, used to create a dependency
        """
        self.__create_slurm_job('clean_data.sbatch', "cleanData")
        with open('clean_data.sbatch', 'a') as f:
            f.write("python Author.py 'Author.remove_duplicate_authors_and_collabs()'")
            f.write("\n")
            f.write("python Institution.py 'Institution.remove_duplicate_institutions()'")
            f.write("\n")
            f.write("python Work.py 'Work.remove_duplicate_works()'")
        if dependency == None:
            ret = self.__execute_slurm_job("clean_data.sbatch")
        else:
            ret = self.__execute_slurm_job("clean_data.sbatch", dependency)
        print("Submitted " + str(ret))
        return ret
    def run_tests_on_cleaning(self, dependency=None):
        """
        Create and submit a job to run multiple tests on the cleaning portion
        This job makes sure that there are no duplicates in the files. it also makes sure that there are
        no objects which are lost between the original files and their editied versions.
        Creates the following sbatch files:
        cleaning_tests.sbatch
        Uses the following methods:
        File.check_for_duplicates()
        File.compare_to_edit
        Author.test_no_duplicates_in_author_and_collab
        Author.check_no_lost_collabs
        Check output files for results

        Requirements
        ------------
        openalex_author_dump.json
        openalex_work_dump.json
        openalex_collab_dump.json
        openalex_institution_dump.json
        openalex_author_dump_edit.json
        openalex_work_dump_edit.json
        openalex_collab_dump_edit.json
        openalex_institution_dump_edit.json

        Parameters
        ----------
        dependency:
            Default value: None
            If this value is not none, the slurm job will be created with a dependency to only
            run after successful completion of the dependency

        Returns
        -------
        return value of the Slurm job, used to create a dependency
        """
        self.__create_slurm_job('check_cleaned_data.sbatch', "cleanCheck")
        with open('check_cleaned_data.sbatch', 'a') as f:
            f.write('''python File.py "File.check_cleaned_data()"''')
            f.write("\n")
            f.write('''python Author.py "Author.check_authors_complete()"''')
        if dependency == None:
            ret = self.__execute_slurm_job('check_cleaned_data.sbatch')
        else:
            ret = self.__execute_slurm_job('check_cleaned_data.sbatch', dependency)
        print("Submitted " + str(ret))
        return ret
    def reset_current_db(self, url, username, password, database, dependency=None):
        """
        If updating a previously existing database, this job is run. It checks to make sure that there
        is not a >10% change in any node number. If this is the case, the update can be run and the current
        database is deleted. Otherwise, an error is thrown and the update does not occur.
        Creates the following sbatch files:
        check_db_counts.sbatch
        Uses the following methods:
        Author.compare_counts_in_db()

        Requirements
        ------------
        openalex_author_dump_edit.json
        openalex_work_dump_edit.json
        openalex_collab_dump_edit.json
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
        dependency:
            Default value: None
            If this value is not none, the slurm job will be created with a dependency to only
            run after successful completion of the dependency

        Returns
        -------
        return value of the Slurm job, used to create a dependency
        """
        self.__create_slurm_job('check_db_counts.sbatch', "checkDBCounts")
        with open('check_db_counts.sbatch', 'a') as f:
            f.write('''python Author.py "Author.compare_counts_in_db('%s', '%s', '%s', '%s', equal=%s)"''' % (url, username, password, database, "False"))
        if dependency == None:
            ret = self.__execute_slurm_job('check_db_counts.sbatch')
        else:
            ret = self.__execute_slurm_job('check_db_counts.sbatch', dependency)
        print("Submitted " + str(ret))
        return ret
    def upload_to_neo4j(self, url, username, password, database, dependency=None):
        """
        Create and submit a job to upload all nodes to Neo4j
        All JSON files are used to upload all nodes to the Neo4j database
        Creates the following sbatch files:
        upload_inst.sbatch
        upload_collabs.sbatch
        upload_authors.sbatch
        upload_works.sbatch
        upload_authored.sbatch
        Uses the following methods:
        Institution.upload_institutions()
        Author.upload_collabs_parallel()
        Author.upload_current_gt_authors()
        Work.upload_works_parallel()
        Work.create_authored_relationship()
        Uses job dependencies to ensure the jobs run in that order

        Requirements
        ------------
        openalex_author_dump_edit.json
        openalex_work_dump_edit.json
        openalex_collab_dump_edit.json
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
        dependency:
            Default value: None
            If this value is not none, the slurm job will be created with a dependency to only
            run after successful completion of the dependency

        Returns
        -------
        return value of the Slurm job, used to create a dependency
        """
        file = File()
        self.__create_slurm_job("upload_inst.sbatch", "uploadInstitutions")
        with open('upload_inst.sbatch', 'a') as f:
            f.write('''python Institution.py "Institution.upload_institutions('%s', '%s', '%s', '%s')"''' % (url, username, password, database))
        if dependency == None:
            ret = self.__execute_slurm_job('upload_inst.sbatch')
        else:
            ret = self.__execute_slurm_job('upload_inst.sbatch', dependency)
        print("Submitted " + str(ret))

        array_size = file.get_max_batch_number_collabs()
        self.__create_parallel_slurm_job("upload_collabs.sbatch", "uploadCollabs", array_size=array_size)
        with open('upload_collabs.sbatch', 'a') as f:
            f.write('''python Author.py "Author.upload_collabs_parallel($SLURM_ARRAY_TASK_ID, '%s', '%s', '%s', '%s')"''' % (url, username, password, database))
        ret = self.__execute_slurm_job('upload_collabs.sbatch', ret)
        print("Submitted " + str(ret))

        self.__create_slurm_job("upload_authors.sbatch", "uploadAuthors")
        with open('upload_authors.sbatch', 'a') as f:
            f.write('''python Author.py "Author.upload_current_gt_authors('%s', '%s', '%s', '%s')"''' % (url, username, password, database))
        ret = self.__execute_slurm_job('upload_authors.sbatch', ret)
        print("Submitted " + str(ret))

        array_size = file.get_max_batch_number_works()
        self.__create_parallel_slurm_job("upload_works.sbatch", "uploadWorks", array_size=array_size)
        with open('upload_works.sbatch', 'a') as f:
            f.write('''python Work.py "Work.upload_works_parallel($SLURM_ARRAY_TASK_ID, '%s', '%s', '%s', '%s')"''' % (url, username, password, database))
        ret = self.__execute_slurm_job('upload_works.sbatch', ret)
        print("Submitted " + str(ret))

        self.__create_slurm_job("upload_authored.sbatch", "uploadAuthored")
        with open('upload_authored.sbatch', 'a') as f:
            f.write('''python Work.py "Work.create_authored_relationship('%s', '%s', '%s', '%s')"''' % (url, username, password, database))
        ret = self.__execute_slurm_job('upload_authored.sbatch', ret)
        print("Submitted " + str(ret))

        # Add collaborated_with relationship
        self.__create_slurm_job("upload_collaborated_with.sbatch", "uploadCollaborated")
        with open('upload_collaborated_with.sbatch', 'a') as f:
            f.write('''python Author.py "Author.create_collaborated_relationship('%s', '%s', '%s', '%s')"''' % (url, username, password, database))
        ret = self.__execute_slurm_job('upload_collaborated_with.sbatch', ret)
        print("Submitted " + str(ret))
        return ret
    def test_db_post(self, url, username, password, database, dependency=None):
        """
        Checks to ensure upload went correctly
        Ensures all objects were uploaded by comparing the counts in the database
        to the source JSON files.
        Creates an sbatch file to submit: test_db.sbatch
        Uses Author.py

        Requirements
        ------------
        openalex_author_dump_edit.json
        openalex_work_dump_edit.json
        openalex_collab_dump_edit.json
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
        dependency:
            Default value: None
            If this value is not none, the slurm job will be created with a dependency to only
            run after successful completion of the dependency

        Returns
        -------
        return value of the Slurm job, used to create a dependency
        """
        self.__create_slurm_job("test_db.sbatch", "testDB")
        with open('test_db.sbatch', 'a') as f:
            f.write('''python Author.py "Author.compare_counts_in_db('%s', '%s', '%s', '%s')"''' % (url, username, password, database))
        if dependency == None:
            ret = self.__execute_slurm_job('test_db.sbatch')
        else:
            ret = self.__execute_slurm_job('test_db.sbatch', dependency)
        print("Submitted " + str(ret))
        return ret
    def rename_files(self, dependency=None):
        """
        Renames JSON files to include timestamp in order to restore database at a later point
        Creates an sbatch file to submit: rename.sbatch
        Uses File.py

        Requirements
        ------------
        openalex_author_dump.json
        openalex_work_dump.json
        openalex_collab_dump.json
        openalex_institution_dump.json
        openalex_author_dump_edit.json
        openalex_work_dump_edit.json
        openalex_collab_dump_edit.json
        openalex_institution_dump_edit.json

        Parameters
        ----------
        dependency:
            Default value: None
            If this value is not none, the slurm job will be created with a dependency to only
            run after successful completion of the dependency

        Returns
        -------
        return value of the Slurm job, used to create a dependency
        """
        self.__create_slurm_job("rename.sbatch", 'renameFiles')
        with open('rename.sbatch', 'a') as f:
            f.write('''python File.py "File.rename_files()"''')
        if dependency == None:
            ret = self.__execute_slurm_job('rename.sbatch')
        else:
            ret = self.__execute_slurm_job('rename.sbatch', dependency)
        print("Submitted " + str(ret))
        return ret
    def print_nums_in_file(self):
        """
        Create and submit a job to print the number of nodes in each file
        Creates the following sbatch files:
        counting.sbatch
        Uses the following methods:
        File.check_num_in_json()
        Check output files for results

        Requirements
        ------------
        openalex_author_dump_edit.json
        openalex_work_dump_edit.json
        openalex_collab_dump_edit.json
        openalex_institution_dump_edit.json

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        if not (File.check_files(False, True)):
            raise FileNotFoundError("You must have all four edited files.")
        self.__create_slurm_job('counting.sbatch', 'counting')
        with open('counting.sbatch', 'a') as f:
            f.write('''python File.py "File.check_num_in_json('%s')"''' % 'openalex_work_dump_edit.json')
            f.write('\n')
            f.write('''python File.py "File.check_num_in_json('%s')"''' % 'openalex_author_dump_edit.json')
            f.write('\n')
            f.write('''python File.py "File.check_num_in_json('%s')"''' % 'openalex_collab_dump_edit.json')
            f.write('\n')
            f.write('''python File.py "File.check_num_in_json('%s')"''' % 'openalex_institution_dump_edit.json')
            f.write('\n')
        job = self.__execute_slurm_job('counting.sbatch')
        print("Submitted " + str(job))
    def check_db_nums(self, url, username, password, database):
        """
        Runs the compare_counts_in_db method to check the number in the database

        Requirements
        ------------
        All edited and original files

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
        self.__create_slurm_job("test_db.sbatch", "testDB")
        with open('test_db.sbatch', 'a') as f:
            f.write('''python Author.py "Author.compare_counts_in_db('%s', '%s', '%s', '%s')"''' % (url, username, password, database))
        ret = self.__execute_slurm_job('test_db.sbatch')
        print("Submitted " + str(ret))
    def __create_slurm_job(self, file_name, job_name="neo4j"):
        """
        Creates an sbatch with the given characteristics
        Uses SlurmJob attributes to create job

        Requirements
        ------------
        None

        Parameters
        ----------
        file_name: str
            Name of the sbatch file (should include .sbatch)
        job_name: str, optional
            Name of the job
            Default value: neo4j

        Returns
        -------
        None
        """
        with open(file_name, 'w') as f:
            f.write("#!/bin/bash")
            f.write("\n")
            f.write("#SBATCH --job-name=" + str(job_name))
            f.write("\n")
            f.write("#SBATCH --account=" + str(self.account))
            f.write("\n")
            f.write("#SBATCH -N" + str(self.num_nodes) + " --ntasks-per-node=" + str(self.cores_per_node))
            f.write("\n")
            f.write("#SBATCH --mem-per-cpu=" + str(self.mem_per_core))
            f.write("\n")
            f.write("#SBATCH -t" + str(self.job_duration))
            f.write("\n")
            f.write("#SBATCH -qinferno")
            f.write("\n")
            f.write("#SBATCH -o ./Report/" + str(job_name) + ".out")
            f.write("\n")
            f.write("cd $SLURM_SUBMIT_DIR")
            f.write("\n")
            f.write("source " + str(self.python_env_location))
            f.write("\n")
    def __create_parallel_slurm_job(self, file_name, job_name="neo4j", array_size=1):
        """
        Creates an array job sbatch file with the given characteristics
        Uses SlurmJob attributes to create job

        Requirements
        ------------
        None

        Parameters
        ----------
        file_name: str
            Name of the sbatch file (should include .sbatch)
        job_name: str, optional
            Name of the job
            Default value: neo4j
        array_size: int, optional
            Size of the array for the array job
            Default value: 1

        Returns
        -------
        None
        """
        with open(file_name, 'w') as f:
            f.write("#!/bin/bash")
            f.write("\n")
            f.write("#SBATCH --job-name=" + str(job_name))
            f.write("\n")
            f.write("#SBATCH --account=" + str(self.account))
            f.write("\n")
            f.write("#SBATCH -N" + str(self.num_nodes) + " --ntasks-per-node=" + str(self.cores_per_node))
            f.write("\n")
            f.write("#SBATCH --mem-per-cpu=" + str(self.mem_per_core))
            f.write("\n")
            f.write("#SBATCH -t" + str(self.job_duration))
            f.write("\n")
            f.write("#SBATCH -qinferno")
            f.write("\n")
            f.write("#SBATCH --array=0-" + str(array_size))
            f.write("\n")
            f.write("#SBATCH -o ./Report/" + str(job_name) + ".out")
            f.write("\n")
            f.write("cd $SLURM_SUBMIT_DIR")
            f.write("\n")
            f.write("source " + str(self.python_env_location))
            f.write("\n")
    def __execute_slurm_job(self, file_name, dependency=None):
        """
        Submits a given job to the cluster

        Requirements
        ------------
        Requires python3

        Parameters
        ----------
        file_name: str
            Name of the sbatch file (should include .sbatch)
        dependency: str
            If there is a dependency, will submit such that the job will not run until
            after the job submitted as a dependency runs successfully
            Default value: None

        Returns
        -------
        The output of the job
        """
        if dependency is None:
            command = ["sbatch", file_name]
            process = Popen(command, stdout=PIPE, stderr=None)
            output = str(process.communicate()[0])
            output = output.split()
            output = output[len(output) - 1].replace("\\n'", "")
            return output
        else:
            command = ['sbatch', '--dependency=afterok:%s' % dependency, file_name]
            process = Popen(command, stdout=PIPE, stderr=None)
            output = str(process.communicate()[0])
            output = output.split()
            output = output[len(output) - 1].replace("\\n'", "")
            return output