"""
azure_helper
---------
The azure_helper module is used to upload data to Azure blob storage within a PALS excecution
Using this package requires the azure-storage-blob package to be installed
    install using the command "conda install -c conda-forge azure-storage-blob"
    or create an environment with "conda env create -f environment.yml"
"""

import sys
import json
import os
import ssl
import urllib.request
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError, ResourceExistsError
import logging
log = logging.getLogger(__name__)

class AzureHelper:
    """
    The AzureHelper object stores the config info
    and establishes a connection to an Azure storage container

    Parameters
    ----------
    config_filename : str
        The filename of a config JSON file which must contain a collection called "azure_info"
            which contains "connection_string" and "container_name"
        The connection string is found under "Access keys" on the Storage account page
        AzureHelper checks to see if a container already exists with contair_name
        If no container exists with that name, then a new container will be created with that name
    """

    def __init__(self, config_filename: str):
        config_filename = '../' + config_filename
        filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)), config_filename)
        with open(filepath) as json_file:
            self.config = json.load(json_file)

        self.config = self.config['azure_info']

        self.blob_service_client = BlobServiceClient.from_connection_string(
            self.config['connection_string']
            )

        container_client = self.blob_service_client.get_container_client(
            self.config['container_name']
            )
        try:
            container_client.get_container_properties()
        except AzureError:
            self.blob_service_client.create_container(self.config['container_name'])


    def upload_data(self,
                    blob_contents,
                    blob_name: str,
                    blob_subdir: str = None,
                    overwrite: bool = True):
        """
        Uploads data from memory to a blob in Azure storage

        Parameters
        ----------
        blob_contents : file pointer, text, binary, etc. in memory
            The content to upload to the blob
            This can be text, binary, file handle, etc
            This should not be a local file name, unless intent is to upload the filename as text
            If the intent is to upload a file from disk, use upload_file instead
        blob_name : str
            The name of the blob
            Creates a new blob if a blob does not exist with the given blob_name
        blob_subdir : str, default None
            The subdirectory within the container where the blob should be located
            This will be appended at the beginning of blob_name
        overwrite : bool, default True
            boolian value which decides if the blob will be overwritten if it already exists
            If overwrite=False and the blob already exists, no data will be uploaded
        """
        if blob_contents is None:
            raise ValueError("blob_contents cannot be None")
        if blob_name is None:
            raise ValueError("blob_name cannot be None")
        if blob_subdir is None:
            blob_subdir = ''

        if blob_subdir == '' or blob_subdir.endswith('/'):
            joiner = ''
        else:
            joiner = '/'
        blob_name = joiner.join((blob_subdir, blob_name))

        # TODO implement Gzip compression

        blob_client = self.blob_service_client.get_blob_client(
            self.config["container_name"], blob_name
            )

        try:
            blob_client.upload_blob(blob_contents, overwrite=overwrite)
        except ResourceExistsError:
            return


    def upload_file(self,
                    local_filename: str,
                    blob_name: str = None,
                    blob_subdir: str = None,
                    overwrite: bool = True):
        """
        Uploads data from a file on disk to a blob in Azure storage

        Parameters
        ----------
        local_filename : str
            The relative filepath of the file to upload
        blob_name : str, default value
            Creates a new blob if a blob does not exist with the given blob_name
            If no blob_name is provided then the name of the local file will be used instead
        blob_subdir : str, default None
            The subdirectory within the container where the blob should be located
            This will be appended at the beginning of blob_name
        overwrite : bool, default True
            boolian value which decides if the blob will be overwritten if it already exists
            If overwrite=False and the blob already exists, no data will be uploaded
        """
        if local_filename is None:
            raise ValueError("local_filename cannot be None")

        local_filename = '../' + local_filename
        if blob_name is None:
            blob_name = local_filename.split('/')[-1]
        if blob_subdir is None:
            blob_subdir = ''

        filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)), local_filename)
        with open(filepath, 'rb') as blob_contents:
            self.upload_data(blob_contents, blob_name, blob_subdir, overwrite=overwrite)

    def endpoint(self,
                 endpoint_url: str,
                 df_tag_data, tag_names,
                 allow_self_signed_https: bool = False):
        """
        Sends data to an ACI endpoint and returns the results (or error)

        Parameters
        ----------
        endpoint_url : str
            The REST endpoint URL found on the endpoint page at ml.azure.com
        df_tag_data : pd.DataFrame
            A dataframe contianing the tag data for this run
        tag_names : List[str]
            A list of the names of the tags which are needed for the model at the endpoint
        allow_self_signed_https : bool
            this part is needed if you use self-signed certificate in your scoring service
            bypass the server certificate verification on client side
         """
        if allow_self_signed_https and not os.environ.get('PYTHONHTTPSVERIFY', '') and getattr(ssl, '_create_unverified_context', None):
            ssl._create_default_https_context = ssl._create_unverified_context

        # check for timestamp in the list of tags
        timestamp_name = None
        for name in ['timestamp', 'timestamps', 'Timestamp', 'Timestamps']:
            if name in tag_names:
                timestamp_name = name
                break

        # transform data into compatible format for endpoint
        data_list = []
        for index in range(len(df_tag_data)):
            temp_dict = {}
            if timestamp_name is not None:
                temp_dict[timestamp_name] = df_tag_data.index[index]
            for tag in tag_names:
                temp_dict[tag] = str(df_tag_data[tag][index])
            # temp_dict['DSFLINE1_SIMULATED_GAS_1'] = '250'
            data_list.append(temp_dict)
        data = {"data": data_list}

        # get api key from the connection string
        api_key = ''
        con_string = self.config['connection_string']
        parsed = con_string.split(';')
        for string in parsed:
            if 'AccountKey' in string:
                api_key = string[12:]
                break

        # formulate and send http request
        headers = {'Content-Type':'application/json', 'Authorization':('Bearer '+ api_key)}
        body = str.encode(json.dumps(data))
        req = urllib.request.Request(endpoint_url, body, headers)
        try:
            response = urllib.request.urlopen(req)
            result = response.read()
            print(result.decode())
            return result.decode()
        except urllib.error.HTTPError as error:
            print("The request failed with status code: " + str(error.code))
            print(error.info())
            print(json.loads(error.read().decode("utf8", 'ignore')))
