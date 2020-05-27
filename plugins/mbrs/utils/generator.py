#   mbrs
#   Copyright (c)Cloud Innovation Partners 2020.
#   http://www.cloudinp.com

"""
This module is a Helper module used by dag_generator to create
user DAGs dynamically using `config` variable from the Airflow
meta-database. It also helps in post installation setps such as
creation of `rest` connection for the application use.
"""

import inspect
import re
import json
import os
import socket
import requests
from jinja2 import Template
from sqlalchemy.exc import IntegrityError
from airflow import configuration
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models.connection import Connection
from airflow.models.variable import Variable
from airflow.hooks.base_hook import BaseHook
from airflow import settings, models
from airflow.exceptions import AirflowConfigException
from plugins.mbrs.modals.recovery_modals import Dags
from plugins.mbrs.utils.dates import get_start_date
from plugins.mbrs.utils.exceptions import (PostgreSQLConnectionNotFoundException,
                                           AirflowException,
                                           MYSQLConnectionNotFoundException,
                                           ServiceNowConnectionNotFoundException,
                                           S3ConnectionNotFoundException,
                                           ConfigVariableNotFoundException,
                                           AirflowAPICredentialsNotFoundException,
                                           SFTPConnectionNotFoundException,
                                           StorageTypeNotFoundException,
                                           InvalidStorageTypeException,
                                           DropboxConnectionNotFoundException,
                                           MSSQLConnectionNotFoundException,
                                           GoogleDriveConnectionNotFoundException,
                                           InvalidTableName)

bootstrap = False
servicenow_default = None
rest = None
config = None
r_config = None
dag_creation_dates = None
storage_type = None
sftp_default = None
s3_default = None
dropbox_default = None
mysql_default = None
new_dags = None


def create_airflow_connection_default_servicenow():
    """
    creates `servicenow_default` connection with default parameters
    :return: None
    """
    session = settings.Session()

    if len(session.query(Connection).filter(Connection.conn_id == 'servicenow_default').all()) == 0:
        connection = Connection(
            conn_id='servicenow_default',
            host='https://dev1234.service-now.com',
            login='admin',
            password='password'
        )

        session.add(connection)
        session.commit()
        session.close()
        LoggingMixin().log.info("conn_id 'servicenow_default' initialized")


def create_airflow_connection_s3_default():
    """
    creates `s3_default` connection with default parameters
    :return: None
    """
    session = settings.Session()

    if len(session.query(Connection).filter(Connection.conn_id == 's3_default').all()) == 0:
        connection = Connection(
            conn_id='s3_default',
            login='<access_key>',
            password='password',
            extra='{"region-name":"ap-south-1","bucket-name":"mini-brs"}'
        )

        session.add(connection)
        session.commit()
        session.close()
        LoggingMixin().log.info("conn_id 's3_default' initialized")


def create_airflow_connection_dropbox_default():
    """
    creates `drop_default` connection with default parameters
    :return: None
    """
    session = settings.Session()

    if len(session.query(Connection).filter(Connection.conn_id == 'dropbox_default').all()) == 0:
        connection = Connection(
            conn_id='dropbox_default',
            login='<access_key>',
            password='password'
        )

        session = settings.Session()
        session.add(connection)
        session.commit()
        session.close()
        LoggingMixin().log.info("conn_id 'dropbox_default' initialized")


def create_airflow_rest_connection():
    """
    creates `rest` connection using airflow.cfg
    :return:
    """
    from airflow.contrib.auth.backends.password_auth import PasswordUser
    import base64

    session = settings.Session()
    exists = session.query(models.User).filter(models.User.username == 'application').scalar()

    if exists is None:

        LoggingMixin().log.info("creating 'application' user for mini-BRS...")

        # create 'application' user

        random_key = str(base64.urlsafe_b64encode(os.urandom(32)))
        user = PasswordUser(models.User())
        user.username = 'application'
        user.email = 'application@nowhere.com'
        user.password = random_key
        session.add(user)
        session.commit()
        session.close()

        # create 'application' airflow connection
        rest = Connection(
            conn_id='rest',
            login='application',
            password=random_key
        )

        session = settings.Session()
        session.add(rest)
        session.commit()
        session.close()

        # create 'admin' user
        # admin_password = str(base64.urlsafe_b64encode(os.urandom(32)))

        config_parser = configuration.AirflowConfigParser()

        config_parser.read(
            configuration.get_airflow_config(configuration.get_airflow_home())
        )

        u = config_parser.get(
            section='core',
            key='username'
        )

        p = config_parser.get(
            section='core',
            key='password'
        )

        user = PasswordUser(models.User())
        user.username = u
        user.email = 'admin@admin.com'
        user.password = p
        user.superuser = True
        session = settings.Session()
        session.add(user)
        session.commit()
        session.close()

        config_parser.remove_option(
            section='core',
            option='username'
        )

        config_parser.remove_option(
            section='core',
            option='password'
        )

        file = open(configuration.get_airflow_config(configuration.get_airflow_home()), 'w')

        config_parser.write(file)

        file.close()


def create_configuration_variables():
    """
    creates default variables for miniBRS usage
    :return:
    """

    # 'config' variable

    Variable.set(
        key='config',
        value=json.dumps({
            "tables": [],
            "start_date": "1day",
            "frequency": "hourly",
            "threshold": 10000,
            "export_format": "xml",
            "storage_type": "sftp",
            "email": ""
        }))

    # 'r_config' variable

    Variable.set(
        key='r_config',
        value='{}'
    )

    # 'dag_creation_dates' variable

    Variable.set(
        key='dag_creation_dates',
        value=json.dumps({})
    )


def ini():

    global bootstrap

    _trace_ = inspect.stack()
    _invoking_function_ = _trace_[len(_trace_)-2].function

    if _invoking_function_ == "initdb" or _invoking_function_ == "resetdb":

        LoggingMixin().log.info("airflow db initialization, starting bootstrap...")

        bootstrap = True

    elif _invoking_function_ == "wrapper":

        LoggingMixin().log.info('mini-BRS running...')
        bootstrap = False
        create_airflow_rest_connection()

    else:
        pass

    return bootstrap


def create_dags():
    """
    This function generates user DAGs using a `config` variable
    :return:
    """

    global dag_creation_dates
    global new_dags
    global email_notify_required

    new_dags = []

    dag_creation_dates = json.loads(Variable.get(key='dag_creation_dates'))
    email_notify_required = is_email_notification_required()

    try:
        if not isinstance(config.get('tables'),list):
            raise InvalidTableName("`tables` attribute of `config` variable should be Array not string or interger")

        regex = re.compile('[@=+!#$%^&*()<>?/\|}{~:;]')
        for table in config.get('tables'):
            if regex.search(table) is None and ',' not in table\
                    and table not in ('.','-','_','') and not table.__contains__(' ') :
                with open(configuration.get_airflow_home() + '/dags/templates/main.py.jinja2') as file_:
                    template = Template(file_.read())

                if dag_creation_dates.get(table) is not None:
                    start_date = dag_creation_dates.get(table)
                else:
                    start_date = get_start_date(config.get('start_date'))
                    dag_creation_dates[table] = str(start_date)

                output = template.render(
                    data={
                        'dag_id': table,
                        'frequency': config.get('frequency'),
                        'storage_type': storage_type,
                        'start_date': start_date,
                        'email_required': email_notify_required
                    }
                )

                with open(configuration.get_airflow_home() + '/dags/generated/dag_'
                          + '{}'.format(table).replace(' ', '_') + '.py', 'w') as f:
                    f.write(output)
                    new_dags.append('dag_' + '{}'.format(table).replace(' ', '_') + '.py')

            else:
                raise InvalidTableName("table name must not contain special characters or spaces.")

        if len(r_config) != 0:

            for table in r_config:
                for exec_date in r_config.get(table):
                    execution_date = str(exec_date).replace(' ', 'T')[0:19]
                    with open(configuration.get_airflow_home()
                              + '/dags/templates/recovery_template.py.jinja2') as file_:
                        template = Template(file_.read())
                        output = template.render(
                            data={
                                'dag_id': table,
                                'frequency': config.get('frequency'),
                                'storage_type': storage_type,
                                'execution_date': execution_date})
                    with open(
                            '{}/dags/generated/r_dag_{}_{}'.format(
                                configuration.get_airflow_home(),
                                table,
                                execution_date
                            ).replace(' ', '_') + '.py', 'w') as f:

                        f.write(output)
                        e = '{}'.format(execution_date).replace(' ', 'T')
                        new_dags.append('r_dag_{}_{}'.format(table, e).replace(' ', '_') + '.py')

        md_dag_ids = settings.Session.query(Dags.dag_id, Dags.fileloc).all()

        for record in md_dag_ids:
            (d_id, loc) = record
            filename = loc[str(loc).rfind('/') + 1:]
            if filename == 'dag_generator.py' or filename == 'dag_cleanup.py':
                continue
            if filename not in new_dags:
                try:
                    if os.path.exists(str(loc)):
                        os.remove(str(loc))
                    else:
                        LoggingMixin().log.warning("{} file doesn't exists !".format(filename))

                    requests.delete(
                        url="http://{}:8080/api/experimental/dags/{}".format(
                            socket.gethostbyname(socket.gethostname()),
                            str(d_id)
                        ),
                        auth=(rest.login, rest.password)
                    )

                    dag_creation_dates.pop(d_id)

                except Exception as e:
                    LoggingMixin().log.error(str(e))
        try:
            Variable.set(key='dag_creation_dates', value=json.dumps(dag_creation_dates))
        except IntegrityError:
            msg = "psycopg2.errors.UniqueViolation duplicate key value violates unique constraint"
            LoggingMixin().log.warning(msg)

    except AirflowException as e:
        if isinstance(e,InvalidTableName):
            raise
        else:
            raise ConfigVariableNotFoundException()


def is_servicenow_default_connection_available():
    """
    checks existence of `servicenow_default` connection
    :raises ServiceNowConnectionNotFoundException
    :return:
    """

    global servicenow_default

    try:
        servicenow_default = BaseHook.get_connection('servicenow_default')
        return True
    except AirflowException:
        raise ServiceNowConnectionNotFoundException('No ServiceNow Instance Registered !')


def is_rest_connection_available():
    """
    checks existence of `rest` connection
    :raises AirflowAPICredentialsNotFoundException
    :return:
    """

    global rest

    try:
        rest = BaseHook.get_connection('rest')
        return True
    except AirflowException:
        raise AirflowAPICredentialsNotFoundException("No Connection with conn_id 'rest' found !")


def is_config_variable_set():
    """
    checks existence of `config` variable
    :raises ConfigVariableNotFoundException
    :return:
    """

    global config

    try:
        config = json.loads(Variable.get("config"))
        return True
    except KeyError:
        raise ConfigVariableNotFoundException("Variable 'config' not found")


def is_recovery_variable_set():
    """
    checks existence of `r_config` variable
    :raises ConfigVariableNotFoundException
    :return:
    """

    global r_config

    try:
        r_config = json.loads(Variable.get("r_config"))
        return True
    except KeyError:
        raise ConfigVariableNotFoundException("Variable 'r_config' not found")


def is_storage_defined():
    """
    checks whether `storage_type` attribute in `config` variable has a value
    :raises StorageTypeNotFoundException
    """

    global storage_type
    global sftp_default
    global s3_default
    global dropbox_default
    global postgres_default
    global mysql_default
    global mssql_default
    global google_drive_default
    try:

        storage_type = str(config['storage_type']).lower()

        if storage_type == 'sftp':

            try:
                sftp_default = BaseHook.get_connection('sftp_default')

            except AirflowException:

                raise SFTPConnectionNotFoundException()

        elif storage_type == 's3':
            try:
                s3_default = BaseHook.get_connection('s3_default')

            except AirflowException:

                raise S3ConnectionNotFoundException()

        elif storage_type == 'dropbox':

            try:
                dropbox_default = BaseHook.get_connection('dropbox_default')

            except AirflowException:

                raise DropboxConnectionNotFoundException()

        elif storage_type == 'postgres':

            try:

                postgres_default = BaseHook.get_connection('postgres_default')

            except AirflowException:

                raise PostgreSQLConnectionNotFoundException()

        elif storage_type == 'mysql':
            try:

                mysql_default = BaseHook.get_connection('mysql_default')

            except AirflowException:

                raise MYSQLConnectionNotFoundException()

        elif storage_type == 'mssql':
            try:
                mssql_default = BaseHook.get_connection('mssql_default')

            except AirflowException:

                raise MSSQLConnectionNotFoundException()

        elif storage_type == 'googledrive':
            try:
                google_drive_default = BaseHook.get_connection('google_drive_default')

            except AirflowException:

                raise GoogleDriveConnectionNotFoundException()

        else:

            raise InvalidStorageTypeException()

    except KeyError:

        raise StorageTypeNotFoundException


def is_email_notification_required():
    """
    checks whether email_notification is required or not
    :return:
    """

    try:
        configuration.get(
            section='smtp',
            key='smtp_user'
        )
        configuration.get(
            section='smtp',
            key='smtp_password'
        )

        return True
    except AirflowConfigException:
        return False
