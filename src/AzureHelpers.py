""" Contains helper methods needed for writing data to Blob storage in Azure """
import sys
import json
import os
from azure.storage.blob import BlobClient

class AzureHelpers:

    ###########################################
    # UploadToBlob
    # Uploads information to a blob in Azure storage
    # Inputs:
    #   config_filename:str = the filename of a config JSON file which must contain connection_string and container_name
    #       The connection string is found under "Access keys" on the Storage account page
    #       The container specified by container_name must already exist
    #   blob_filename:str = the name of the blob
    #       Creates a new blob if a blob does not exist with the given blob_filename 
    #       Overwrites the existing blob if a blob already exists with the given blob_filename
    #   blob_contents = the content to upload to the blob
    #       This can be text, binary, file handle, etc
    #   blob_subdir:str = the subdirectory within the container where the blob should be located
    #       This will be appended at the beginning of blob_filename
    ###########################################
    def UploadToBlob(self, config_filename:str, blob_filename:str, blob_contents, blob_subdir:str = ''):
        
        config = self.LoadConfigFile(config_filename)
        blob_client = BlobClient.from_connection_string(
            config["connection_string"],
            config["container_name"],
            blob_subdir + blob_filename)
        blob_client.upload_blob(blob_contents, overwrite=True)

    ###########################################
    # LoadConfigFile
    ###########################################
    def LoadConfigFile(self, config_filename):

        filepath = os.path.join(os.path.dirname(os.path.realpath(__file__)), config_filename)

        with open(filepath) as json_file: 
            config = json.load(json_file)

        return config