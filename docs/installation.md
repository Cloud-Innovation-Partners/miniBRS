Installing miniBRS
==================
miniBRS can be installed either using an Installer script or manually. Using the Installer is the easiest and straightforward way to install and configure miniBRS. If you are using Ubuntu 18.04 you can install using Installer
script, For any other OS, you would have to do the installation and configuration manually.

---

Prerequisites
-------------


**Database**

You need a database on your database server. If you don’t have a database yet, you can use the SQLite database which is default database of Airflow. This will allow you to test miniBRS even before your database administrator creates a database for you. `Just note, that SQLite is not suitable for production environment`.


mini-BRS is tested with

* Postgres DB: 10.12
* MySQL DB: 8.0
* SQLite - latest stable (it is used mainly for testing purpose)


**Email Server**

You would need an `Email Server` in case you want email alerts for your workflows, 
Make sure you have SMTP server details like `smtp_host`, `smtp_port`, `email address` and `password` in handy.
If you don't have an Email Server, you can use Gmail, Outlook or any other email provider make sure you generate 
`app password` for your email address. To know how to generate `app password` for your Gmail account, refer
[here](https://support.google.com/accounts/answer/185833?hl=en)  

---

Installation
------------

**Using Installer Script**

1. clone the repository using `git`
        
        ~$ git clone https://github.com/Cloud-Innovation-Partners/miniBRS.git

2. execute ```install``` script inside the project folder using ```sudo``` command 

        ~$ sudo ./install

The installation script will take care of your needs, it will start downloading the dependencies and requirements for the
project. During installation, you will need to provide various information such as,

1. The installer will prompt you for the type of installation, You can either install mini-BRS as a Ubuntu service or you can let the installer create a python virtual environment for you.

2. Next, it will ask you for the type of database to be used as a meta-database for Airflow, mini-BRS is tested for SQLite, MySQL and Postgres. You can choose the type of database and provide the specific database credentials during installation.

3. `miniBRS` provide failure alerts option via email, you can provide the SMTP server to be used for sending failure notifications, incase, workflows fail. You can choose to skip this step If you don't want email alerts.

4. You will be prompted to create your Airflow UI login credentials, by default username is ```admin``` you can keep 
    the default username and add a password to it.

Once the installation is over, If you have chosen to install mini-BRS as service than after installation you will have two
system services installed one for ```airflow-webserver``` and other for ```airflow-scheduler``` active and running on
your machine. You can check the status of the service by

```bash
~$ sudo service airflow-websever status
``` 


```bash
~$ sudo service airflow-scheduler status
``` 

If you have installed mini-BRS as a python virtual environment then you will be having a folder named ```.env``` created
in ```miniBRS``` folder. This folder is the python virtual environment and you can use the following command to activate it

>make sure your current working directory is ```miniBRS``` 

```bash
~$ source .env/bin/activate
``` 
Once you activate your virtual environment you can start Airflow webserver and scheduler 


```bash
~$ airflow webserver
``` 


```bash
~$ airflow scheduler
``` 

After you have finished the installation process and you have started airflow webserver and scheduler. 
You can access **`miniBRS`** UI via [**`http://locahost:8080`**](http://locahost:8080)

---

Manual Installation
-------------------
> **Prerequisite:** Linux OS, Although any Linux distro would work we prefer Debian distros e.q Ubuntu, Debian etc.  

>For **Red Hat** Linux the process of installation would remain same with slight changes specific to the use of corresponding package manager

>**Windows:** If you are a windows user and you want to run Apache Airflow on windows platform, you would need extra housekeeping, For windows platform, you would need a Virtual Machine simulator like *VirtualBox* / *VMware* for running Linux on Windows or if you are using Windows 10 then you can use the **Windows Sub System For Linux** to have your airflow installed. In any case, the internet is yours go and find it :wink:  

<pre>
 ____  _                 
/ ___|| |_ ___ _ __  ___ 
\___ \| __/ _ \ '_ \/ __|
 ___) | ||  __/ |_) \__ \
|____/ \__\___| .__/|___/
              |_|        
</pre>


1. **Git Installation:** In order to fetch ``miniBRS`` project from GitHub, you need to have ``git`` installed in your machine. To check if ``git`` is installed, run the below command in your terminal
    
        ~$ git version
    
    The output should be something like this :
    
        git version 2.17.0  
  
    The version number may differ depending on your machine and time of installation. If the output display's 
    the version number that means you have ``git`` installed.
    
    In case, ``git`` is not installed. you can install it by the following command
    
        ~$ sudo apt-get install git
    <br/>
    
2. **Download miniBRS:** Once ``git`` is installed, you can fetch the project from the `GitHub` repo using below command.
    
        ~$ git clone https://github.com/Cloud-Innovation-Partners/miniBRS.git
   
    <br/>

3. **Create Virtual Environment:** Now, change your directory to ``miniBRS`` and enter the following command to create 
     a virtual environment.

    **<u>Make sure you are using Python 3.x and not Python 2.7</u>**
    
        ~$ python -m venv .env
    
        or 
    
    
        ~$ python -m virtualenv .env
    
    In case, python throws no module ``virtualenv`` found, you need to install the ``virtualenv`` package, to install ``virtualenv`` package using below command

        ~$ pip install virtualenv
    
    or
    
        ~$ pip3 install virtualenv
    
    <br/>

4. **Set Environment Variable:** Before installing the dependencies you need to activate your virtual environment and also set a ``PATH`` variable with the name ``AIRFLOW_HOME`` it's necessary for ``miniBRS`` to find the project files. 
    Let's do it one by one.
    
    with ``miniBRS`` as your ``cwd`` hit the below command in terminal

        ~$ pwd
    
    This will give you an output something like this
    
        /home/<YOUR USER NAME HERE>/airflow_workspace/miniBRS
        
    copy this output as we need it to set our ``PATH`` variable
    
    Now use the following command to open an editor
    
        ~$ nano .env/bin/activate
        
    This will open a bash script, scroll to the end of the file and paste the following lines at the end of the file
    
        #This is for AIRFLOW usage
        export AIRFLOW_HOME=/home/<YOUR USER NAME HERE>/airflow_workspace/miniBRS
        
    press `CTRL+X` and `y`, to close the editor
    
    You are ready to activate your virtual environment. enter the following command to activate it

        ~$ source .env/lib/activate
        
    <br/>

5. **Install Dependencies:** To install project dependencies, we prefer installation using ``requirements.txt`` file. 
    In ``miniBRS`` project folder you have a file ``requirement.txt`` which has all the project dependencies list in it. 
    enter the following command to install dependencies

        ~$ pip3 install -r requirement.txt
    
    <br/>

6. **Backup DAG's Folder:** Before we initialize ``airflow`` meta-database. Lets back up our workflows. Hit the following commands in the terminal. make sure you have `miniBRS` as the current working directory

        ~$ mv -r dags dags_bak
    <br/>
    
7. **Initialize Airflow Meta-Database:** To initialize `meta-database` use this command
        
        ~$ airflow initdb
        
     The initialization of meta-database results in the creation of following files in `miniBRS` project folder
     `airflow.cfg`, `airflow.db` and `unittest.cfg`. We will get back to these files in separate sections
     
     <br/>
     
8. **Create Default Connections:** `miniBRS` use various connections and variables for its functioning. It is necessary to
    create default connections before you can use `miniBRS` If the connections are not created `miniBRS` will throw
    exceptions. Creating connections is just a matter of a few commands. Let us create them via terminal only, Although you
    can create them from UI as well check out the **[User Interface](user_interface.md#connections)** section for more details on how to create connections
    and variables from UI
    
    * **servicenow_default:** This connection is used to store your `Service Now` Instance details, feel free to add values to the options in the below command as per your own Instance details. You can provide your service now instance URL
        to --conn_host, Your Service Now username to --conn_login and Service Now password to --conn_password options
    
            ~$ airflow connections -a --conn_id servicenow_default --conn_type None --conn_host https://dev1234.service-now.com --conn_login admin --conn_password None
     
    * **s3_default:** This connection is used to store your `Amazon S3` credentials, feel free to add values to the options 
    or you can create the connection with dummy values and later change them from UI
    
            ~$ airflow connections -a --conn_id s3_default --conn_type None --conn_login access_key_id --conn_password None --conn_extra '{"region-name": "ap-south-1", "bucket-name": "mini-brs"}'
            
    * **dropbox_default :** This connection is used to store your `DropBox` access_token
    
            ~$ airflow connections -a --conn_id dropbox_default --conn_type None --conn_login None --conn_password None
            
    * **google_drive_default :** This connection is used to store your `Google Drive` access_token
    
            ~$ airflow connections -a --conn_id google_drive_default --conn_type None --conn_login None --conn_password None --conn_extra '{"access_token": "<YOUR-ACCESS-TOKEN_HERE>","scope": "https://www.googleapis.com/auth/drive","token_type": "Bearer","expires_in": 3599,"refresh_token": "<YOUR-REFRESH-TOKEN_HERE>"}' 
            
9. **Create Default Variables:** Like default connections `miniBRS` also uses few default variables for its functioning
    hit copy paste for below command's
    
    * **config** This is a configuration variable used to customize your workflows
            
            ~$ airflow variables -s config '{"tables": [], "start_date": "1day", "frequency": "hourly", "threshold": 10000, "export_format": "xml", "storage_type": "sftp", "email": ""}'
            
    * **r_config** This is a system variable, used for functioning of `miniBRS`
        
            ~$ airflow variables -s r_config '{}'
            
    * **dag_creation_dates :** Another system variable
            
            ~$ airflow variables -s dag_creation_dates '{}'
            
10. **Populate DAGs:** Now is the time to get our DAG's populated. In Step 6 we made a backup of DAG's let get them back
    in context, use following commands to rename `dags_bak` to `dags` 
    
            ~$ rm -r dags           
            ~$ mv dags_bak dags
            
11. **Start Airflow Web Server & Scheduler:** Congratulations, you have made up to this step, Now is the time to start the webserver and the scheduler. To start webserver use following command
    
            ~$ airflow webserver
    
    To start `scheduler`, use command
            
            ~$ airflow scheduler
            
12. **Access miniBRS UI:** After you have finished the installation process and you have started airflow webserver and scheduler. 
    You can access **`miniBRS`** UI via [**`http://locahost:8080`**](http://locahost:8080)
            
**Important Notes:**

1. The Manual Installation makes use of `SQLite` database, This is not recommended for production purpose
2. In order to change the database you need to change a configuration value for `sql_alchemy_conn` in the `airflow.cfg` 
    file present in the `miniBRS` project folder. This key `sql_alchemy_conn` take `SQLAlchemy` connection string as an argument you can find a link in the references to tweak these settings. 
    
3. Also, Manual Installation does not speak about the installation of `miniBRS` as a service, This is something which installer does. But if you need to install `miniBRS` as a service don't forget to check references link for your help.
    

## References

* [Installing Database Other than SQLite for Airflow](https://airflow.apache.org/docs/stable/installation.html#initiating-airflow-database)
* [Installing Airflow as a Service](https://medium.com/@shahbaz.ali03/run-apache-airflow-as-a-service-on-ubuntu-18-04-server-b637c03f4722)     
