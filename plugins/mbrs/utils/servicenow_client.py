#  mbrs
#  Copyright (c)Cloud Innovation Partners 2020.
#  http://www.cloudinp.com

import requests,base64,simplejson
from xml.etree import ElementTree
import xml
import logging


#create a logger
from plugins.mbrs.utils.exceptions import InvalidArguments

log = logging.getLogger(__name__)
#set log level
log.setLevel(logging.INFO)

#create a log handler
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)

#create a formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#set formatter to handler
handler.setFormatter(formatter)

#add handler to logger
log.addHandler(handler)

def is_empty(arg):
    arg=str(arg) # if arg is the type of date object, convert it into string first
    if len(arg.strip()) == 0:
        return True
    else:
        return False


class ServiceNowClient(object):

    '''
    The ServiceNowClient makes API Calls to the `ServiceNow REST API <https://developer.servicenow.com/app.do#!/rest_api_doc?v=madrid&id=r_TableAPI-GET>`_ as well as
    managing connections for the API calls_

    It also manages OAuth authentication process for servicenow instance.
    ServiceNowClient manages the creation and validation of OAuth and Basic authorization and authetication
    process related to Web API's of ServiceNow

    ServiceNowClient is managed and developed by CloudBRS for more info check <http://developers.cloudbrs.com>



    @:param: host : service-now instance
    @:param: login : service-now username
    @:param: password: service-now user login password
    @:param: auth_type: Authetication Type
    @:param: token: servic-now access_token
    @:param: refresh_token: service-now refresh_token
    @:param: client_id : service-now client_id
    @:param: client_secret : service-now client_secret
    @:type: host : str
    @:type: login : str
    @:type: password: str
    @:type: auth_type: int
    @:type: token: str
    @:type: refresh_token: str
    @:type: client_id: str
    @:type: client_secret: str

    BASIC_AUTH :
            To access API's using BASIC authorization we need
                - host,login,password and auth_type=0
    BEARER_AUTH:
            To access API's using OAuth authorization we may have
                -auth_type=1,host,token (if user already has token)

    TOKEN_GENERATION:
            To generate tokens for OAuth
                -host,login,password,client_secret,client_id (if user doesn't have tokens and needs to generate them)
                -host,logn,password,refresh_token,token
    '''



    AUTH_TYPE_BASIC = 0
    AUTH_TYPE_BEARER = 1


    def __init__(self,
                host=None,
                login=None,
                password=None,
                auth_type=AUTH_TYPE_BASIC,
                token=None,
                refresh_token=None,
                client_id=None,
                client_secret=None,
                **kwargs
                 ):

        msg = 'Cannot access ServiceNow: No valid ServiceNow credentials supplied. ' \
              'add host and login details to the client call'


        # check for empty
        if is_empty(host) or is_empty(login):
            raise InvalidArguments("host, login can't be empty")

        # check for none
        if host == None or login == None:
            raise InvalidArguments("table_name, execution_date can't be None")

        #Using Basic Authentication

        if auth_type == self.AUTH_TYPE_BASIC:

            #Necessary parameters for basic_auth
            if host is None or login is None or password is None:
                raise ServiceNowException(msg)

        #Using OAuth Authentication
        if auth_type == self.AUTH_TYPE_BEARER:

            #Necessary parameters for oauth_authentication
            if token is None:

                log.warning("Cannot access ServiceNow: No valid ServiceNow OAuth token supplied for {}".format(host) )



        #check validity of credentials

        self.auth_type = auth_type
        self.login = login
        self.password = password
        self.host = host
        self.token = token
        self.refresh_token = refresh_token
        self.access_token_expires_at = 0
        self.client_id = client_id
        self.client_secret = client_secret

        # self.__check_validity_of_credentials(
        #     host=host,
        #     login=login,
        #     password=password,
        #     auth_type=auth_type,
        #     token=token,
        #     refresh_token=refresh_token,
        #     client_id=client_id,
        #     client_secret=client_secret,
        #     **kwargs
        # )


    def __check_validity_of_credentials(self,
                                        host=None,
                                        login=None,
                                        password=None,
                                        auth_type=AUTH_TYPE_BASIC,
                                        token=None,
                                        refresh_token=None,
                                        client_id=None,
                                        client_secret=None,
                                        **kwargs
                                        ):

        '''
        The method is used to check the validity of the service-now credentials passed as arguments, the validation is done
        by simple using the credential details passed on to this method for accessing the sys_db_object of the
        instance and checking the count of table present in the instance.
        :param host:
        :param login:
        :param password:
        :param auth_type:
        :param token:
        :param refresh_token:
        :param client_id:
        :param client_secret:
        :param kwargs:
        :return:
        '''


        default_query_args = {
            'headers' : {
                'Accept': 'application/json',
                'Authorization' : "",
                'client_id' : client_id,
                'client_secret' : client_secret,
                'username' : login,
                'password' : password

            },

            'route' : '/api/now/stats/sys_db_object',

            'query_param':{
                  'sysparm_count' : 'true'
            }

        }

        # BASIC Authentication

        if auth_type is self.AUTH_TYPE_BASIC:

            #get count of tables from sys_db_object of instance
            default_query_args['headers']['Authorization'] = "{} {}".format('Basic ', str(base64.b64encode(str(login+":"+password).encode()), 'utf-8'))
            try:
                rs = requests.get\
                        (
                            host+default_query_args.get('route'),
                            params=default_query_args.get('query_param'),
                            headers=default_query_args.get('headers')
                    ).json()

                if 'error' in rs:
                    raise ServiceNowAPIException(rs['error'])
                else:
                    log.info("ServiceNow Basic Auth Credentials OK for {} !".format(self.host))

            except simplejson.errors.JSONDecodeError:
                raise ServiceNowHibernateException(host+' Instance is hibernated !')

        else:

            default_query_args['headers']['Authorization'] = "{} {}".format('Bearer',token)
            try:
                response = requests.get \
                        (
                        host + default_query_args.get('route'),
                        params=default_query_args.get('query_param'),
                        headers=default_query_args.get('headers')
                    )
                rs = response.json()

                if 'error' in rs:

                    #check if servicenow returns Un-Authorized Access
                    if response.status_code == 401:

                        raise ServiceNowAPIException("Access Token Not Valid :"+str(rs['error']))

                else:

                    log.info("ServiceNow Oauth Credentials OK for {} !".format(self.host))

            except simplejson.errors.JSONDecodeError:
                raise ServiceNowHibernateException(host + ' Instance is hibernated !')




    def fetchAccessToken(self,
                          client_id= None,
                          client_secret=None,
                          login=None,
                          password=None,
                          refresh_token=None,
                          ):
        '''
        This methhod is used to generate access_token and refresh_token using client_id and client_secret
        as per the ServiceNow document OAuth request requires a user login details to be passed along side
        to be used for authentication of tokens and for defining ACL for the tokens generated
        :param host:
        :param client_id:
        :param client_secret:
        :param login:
        :param password:
        :param refresh_token:
        :return: access_token
        '''

        if client_id is not None and client_secret is not None :

            default_query_args = {

                'data' : {
                    'grant_type' : 'password' if refresh_token is None else 'refresh_token',
                    'client_id' : client_id,
                    'client_secret' : client_secret,
                    'username' : login,
                    'password' : password,
                    'refresh_token':refresh_token
                },
                'route' : '/oauth_token.do'

            }

            response = requests.post(
                url="{}{}".format(self.host,default_query_args.get('route')),
                data=default_query_args.get('data')
            )

            try:
                rs = response.json()
            except simplejson.errors.JSONDecodeError as e:
                raise ServiceNowHibernateException("Instance Hibernated")
            if 'error' in rs:

                raise ServiceNowAPIException(rs['error'])
            else:
                self.token = rs['access_token']
                self.refresh_token = rs['refresh_token']
                self.access_token_expires_at = int(rs['expires_in'])

                log.info("ServiceNow access_token Generated Successfully for {} !".format(self.host))

                return rs['access_token']

        else:
            raise ServiceNowException("ServiceNow client_id and client_secret not found")

    def get_access_token(self):
        return self.token

    def get_refresh_token(self):
        return self.refresh_token

    def table_schema(self, table_name):
        """
        This method is used to get the schema of the service now table
        :param table_name:
        :type str
        :return: list containing dict() items with column name, type and size
        :type : generator
        """
        default_query_args = {
            'headers': {
                'Authorization': "Basic {}".format(str(base64.b64encode(str(self.login + ":" + self.password).encode()),
                                                    'utf-8')) if self.auth_type == 0 else f"Bearer {self.token}"
            },
            'route': f'/{table_name}.do?SCHEMA',
        }
        response = requests.get(
            url=f"{self.host}{default_query_args.get('route')}",
            headers=default_query_args.get('headers'),
        )
        try:
            root = ElementTree.fromstring(response.text)
        except ElementTree.ParseError:
            raise ServiceNowHibernateException()

        if root.tag == 'error':
            raise ServiceNowAPIException(root.text)

        for element in root.findall('element'):
            yield {
                'name': element.get('name'),
                'type': element.get('internal_type'),
                'reference':None if not element.get('reference_table') else element.get('reference_table'),
                'size': element.get('max_length')
            }





    def api_call(self,method='GET',route=None,query_params=None,accept=None):

        '''
        The api_call() method is used to call the service-now api's
        :param method: pass the subsequent CRUD method
        :param route: pass the route of the API
        :param query_params: pass a dictionary of query parameter
        :type method: str
        :type route: str
        :type query_params :dict
        :return: <xml>response</xml> of the api call
        '''


        if not isinstance(query_params,dict):
            raise ServiceNowAPIException("ServiceNowClient query_params argument to method api_call() must be of type dict")

        if accept is not None :
            if (not str(accept).__eq__("application/json")) or (str(accept).__eq__('application/xml')):
                raise ServiceNowAPIException("ServiceNowClient : accept can have only two value application\\json or application\\xml")

        if route is not None:

            default_query_args = {
                'headers': {
                    'Accept': 'application/xml' if accept is None else str(accept),
                    'Authorization': "{} {}".format('Basic ',str(base64.b64encode(str(self.login+":"+self.password).encode()), 'utf-8')) if self.auth_type == 0 else "{} {}".format('Bearer ',self.token)
                },
                'route': route,
                'query_param': query_params
            }

            m = str(method).upper()
            if m == 'GET':
                response = requests.get(
                    url= '{}{}'.format(self.host,default_query_args.get('route')),
                    headers=default_query_args.get('headers'),
                    params=default_query_args.get('query_param')
                )

                if response.status_code == 401:
                    raise ServiceNowAPIException("Required to provide Auth information, User Not Authenticated ")

                if str(response.headers['Content-Type']).__eq__('text/html'):
                    raise ServiceNowHibernateException

            elif m == 'POST':
                response = requests.post(
                    url=self.host+default_query_args.get('route'),
                    data=default_query_args.get('query_param'),
                    headers=default_query_args.get('headers')
                )
                if response.status_code == 401:
                    raise ServiceNowAPIException("Required to provide Auth information, User Not Authenticated ")
            elif m == 'PUT':
                raise ServiceNowClient("NOT IMPLEMENTED YET")
            elif m == 'DELETE':
                raise ServiceNowClient("NOT IMPLEMENTED YET")

            else:
                raise ServiceNowAPIException("ServiceNowClient uknown method {} found".format(method))

            return response.text
        else:
            raise ServiceNowAPIException("ServiceNowClient no route defined")

class ServiceNowException(Exception):
    '''
    @:exception : ServiceNowException : parent exception for all client related exceptions
    '''
    pass

class ServiceNowHibernateException(ServiceNowException):
    '''
    @:exception : ServiceNowHibernateException : denotes instance is hibernated
    '''
    def __init__(self,*args):
        if args:
            self.message=args[0]
        else:
            self.message='ServiceNow Instance in Hibernated !'

    def __str__(self):
        if self.message:
            return "ServiceNowHibernateException, {}".format(self.message)
        else:
            return "ServiceNowHibernateException has been raised"

class ServiceNowAPIException(ServiceNowException):
    '''
    @:exception : ServiceNowAPIException : raised when API's error's occur
    '''

    def __init__(self, *args):
        if args:
            self.message = args[0]
        else:
            self.message = 'ServiceNow API Error !'

    def __str__(self):
        if self.message:
            return "ServiceNowAPIException, {}".format(self.message)
        else:
            return "ServiceNowAPIException has been raised"

