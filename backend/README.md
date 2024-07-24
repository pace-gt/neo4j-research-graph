# How to Use Preprocessing Code
## Connect to PACE Cluster
The first step is to log onto the Phoenix cluster. If you do not already have an account, faculty can request an account by emailing pace-support@oit.gatech.edu. Indicate your desire to access the Neo4j database in the email. <br>
The next step is to activate the Georgia Tech VPN. If you do not already have the VPN, you can download it at this link: <br>
https://vpn.gatech.edu/global-protect/getsoftwarepage.esp <br>
Activate the VPN and log on with your GT credentials. <br>
More information on downloading the VPN can be found here: <br>
https://gatech.service-now.com/home?id=kb_article_view&sysparm_article=KB0042169 <br>
Once the VPN is active, you can connect to the cluster with the SSH tool. Open command line and type the following command: <br>
```
ssh someuser3@login-phoenix.pace.gatech.edu
# Use your GT username in place of "someuser3"
# press <return> to run
# Must be on GT campus internet or connected to Georgia Tech VPN`
```
Next, type your GT password and press enter. Note that on Linux the password is not displayed as you type. You are now successfully connected to the cluster.
<br>
## Set Up Environment
Once logged onto the cluster, use the ls command to view the current directories. Navigate to the directory containing "p-pace-user" using the cd command. Then, use the following command to create a virtual environment:
```
python3 -m venv /storage/coda1/p-pace-user/0/someuser3/test_installs/neo4j_venv
```
Make sure to replace someuser3 with your own information. Activate the environment with the following command:
```
source /storage/coda1/p-pace-user/0/someuser3/test_installs/neo4j_venv/bin/activate
```
Next, install the necessary imports using the following commands:
```
python -m pip install neo4j
python -m pip install requests
```
At this point you can also install any other desired imports. Next, navigate to your desired director and clone the repository.
<br>
## Running Code
To create the database, navigate to the Create_Database directory and run the following command:
```
python main.py <slurm_account> <environment_location> <#_of_nodes> <#_of_cores_per_node> <mem_per_cpu> <duration> <neo4j_url> <neo4j_username> <neo4j_password> <neo4j_database> <is_database_empty>
```
Replace the arguments with the correct values. If you have been following this tuturial, an example call would be:
```
python main.py "phoenix-testusers" "/storage/coda1/p-pace-user/0/<username>/test_installs/neo4j_venv" 1 4 "7G" "02:00:00" "neo4j+ssc://neo4j-prod.pace.gatech.edu:7687" <username> <password> vip-vxg-summer2023 True
```
The first 6 arguments are for the creation of the sbatch scripts necessary to complete jobs. For more information on these, click the following link: <br>
https://docs.pace.gatech.edu/phoenix_cluster/slurm_guide_phnx/ <br>
The next 4 arguments are the connect URL of the Neo4j database, your Neo4j username and password, and the database you want to upload to. For more information, click this link. <br>
https://gatech.service-now.com/home?id=kb_article_view&sysparm_article=KB0042169 <br>
The final parameter, <is_database_empty>, specifies if your are uploading to a new database or updating a new one. If False, then the script will check if the node counts have changed by more then 10%. If they have, the update will not continue so that human intervention can determine why the change ocurred. If True, the script will skip this step.
