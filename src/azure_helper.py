"""
azure_helper
---------
The azure_helper module is used to upload data to Azure blob storage within a PALS excecution

 TODO clean up documentation: remove parameters and returns UNO
"""

import json
import os
from azure.storage.blob import BlobServiceClient

class AzureHelper:
    """
    The AzureHelper object stores the config info
    and establishes a connection to an Azure storage container

    Parameters
    ----------
    config_filename : str
        The filename of a config JSON file which must contain connection_string and container_name
        The connection string is found under "Access keys" on the Storage account page
        Checks to see if a container already exists with contair_name
        If no container exists with that name, then a new container will be created with that nam

    """

    def __init__(self, config_filename: str):
        filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)), config_filename)
        with open(filepath) as json_file:
            self.config = json.load(json_file)

        self.blob_service_client = BlobServiceClient.from_connection_string(
            self.config['connection_string']
            )

        container_client = self.blob_service_client.get_container_client(
            self.config['container_name']
            )
        try:
            container_client.get_container_properties()
        except:
            self.blob_service_client.create_container(self.config['container_name'])


    def upload_data(self,
                    blob_contents,
                    blob_name: str,
                    blob_subdir: str = None,
                    overwrite: bool = True):
        """
        Uploads data to a blob in Azure storage

        Parameters
        ----------
        blob_contents : file pointer, text, binary, etc.
            The content to upload to the blob
            This can be text, binary, file handle, etc
            This cannot be a local file name (unless the filename should be all that's uploaded)
            Instead use upload_file
        blob_name : str
            The name of the blob
        blob_subdir : str, default None
            The subdirectory within the container where the blob should be located
            This will be appended at the beginning of blob_name
        overwrite : bool, default True
            boolian value which decides if the blob will be overwritten if it already exists
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

        # TODO check to see if blob_contents is a filename
        # TODO catch  If set to False, the operation will fail with ResourceExistsError
        # TODO implement Gzip compression

        blob_client = self.blob_service_client.get_blob_client(
            self.config["container_name"], blob_name
            )
        blob_client.upload_blob(blob_contents, overwrite=overwrite)


    def upload_file(self,
                    local_filename: str,
                    blob_name: str = None,
                    blob_subdir: str = None,
                    overwrite: bool = True):
        """
        Uploads a file to a blob in Azure storage

        Parameters
        ----------
        local_filename : str
            The relative filepath of the file to upload
        blob_name : str, default value
            Creates a new blob if a blob does not exist with the given blob_name
            If no blob_name is provided then the name of the local file will be used instead
        blob_subdir : str, default None
            This will be appended at the beginning of blob_name
        overwrite : bool, default True
            boolian value which decides if the blob will be overwritten if it already exists
        """
        if local_filename is None:
            raise ValueError("local_filename cannot be None")
        if blob_name is None:
            blob_name = local_filename.split('/')[-1]
        if blob_subdir is None:
            blob_subdir = ''

        # TODO catch  If set to False, the operation will fail with ResourceExistsError

        filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)), local_filename)
        with open(filepath, 'rb') as blob_contents:
            self.upload_data(blob_contents, blob_name, blob_subdir, overwrite=overwrite)
