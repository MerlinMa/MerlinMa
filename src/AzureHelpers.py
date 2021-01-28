import sys
import json
import os
from azure.storage.blob import BlobServiceClient, BlobClient

class AzureHelpers:

    ###########################################
    # __init__
    #   Creates an AzureHelpers object
    # Inputs:
    #   config_filename:str = the filename of a config JSON file which must contain connection_string and container_name
    #       The connection string is found under "Access keys" on the Storage account page
    #       Checks to see if a container already exists with contair_name
    #       If no container exists with that name, then a new container will be created with that name
    ###########################################
    def __init__(self, config_filename:str):
        filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)), config_filename)
        with open(filepath) as json_file: 
            self.config = json.load(json_file)

        self.blob_service_client = BlobServiceClient.from_connection_string(self.config['connection_string'])

        try:
            self.blob_service_client.get_container_client(self.config['container_name']) 
        except Exception as ex:
            print('Caught exception: ' + str(ex))
            self.blob_service_client.create_container(self.config['container_name'])

    ###########################################
    # UploadDataToBlob
    #   Uploads data to a blob in Azure storage
    # Inputs:
    #   blob_contents = the content to upload to the blob
    #       This can be text, binary, file handle, etc
    #       This cannot be a local file name (unless you intend to upload just the file name to the blob)
    #       Instead use UploadFileToBlob
    #   blob_name:str = the name of the blob
    #       Creates a new blob if a blob does not exist with the given blob_name 
    #       Overwrites the existing blob if a blob already exists with the given blob_name
    #   blob_subdir:str = the subdirectory within the container where the blob should be located
    #       This will be appended at the beginning of blob_name
    ###########################################
    def UploadDataToBlob(self, blob_contents, blob_name:str, blob_subdir:str = None):
        if blob_contents is None: raise ValueError("blob_contents cannot be None")
        if blob_name is None: raise ValueError("blob_name cannot be None")
        if blob_subdir is None: blob_subdir = ''

        if blob_subdir=='' or blob_subdir.endswith('/'):
            joiner = ''
        else:
            joiner = '/'
        blob_name = joiner.join((blob_subdir, blob_name))

        # TODO check to see if a filename has been passed instead of actual blob contents
        # print(str(type(blob_contents)))
        # if str(type(blob_contents)) in ["<class 'str'>"]:

        blob_client = self.blob_service_client.get_blob_client(self.config["container_name"], blob_name)
        blob_client.upload_blob(blob_contents, overwrite=True)

    ###########################################
    # UploadDataToBlob
    #   Uploads data to a blob in Azure storage
    # Inputs:
    #   local_filename:str = the relative filepath of the file to upload
    #   blob_name:str = the name of the blob
    #       Creates a new blob if a blob does not exist with the given blob_name 
    #       Overwrites the existing blob if a blob already exists with the given blob_name
    #       If no blob_name is provided then the name of the local file will be used instead
    #   blob_subdir:str = the subdirectory within the container where the blob should be located
    #       This will be appended at the beginning of blob_name
    ###########################################
    def UploadFileToBlob(self, local_filename:str, blob_name:str = None, blob_subdir:str = None):
        if local_filename is None: raise ValueError("local_filename cannot be None")
        if blob_name is None: blob_name = local_filename.split('/')[-1]
        if blob_subdir is None: blob_subdir = ''

        filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)), local_filename)
        with open(filepath, 'rb') as blob_contents:
            self.UploadDataToBlob(blob_contents, blob_name, blob_subdir)