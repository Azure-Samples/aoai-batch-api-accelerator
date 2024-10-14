from Utilities import Utils
from AzureStorageHandler import StorageHandler
from AOAIHandler import AOAIHandler
from AzureBatch import AzureBatch
import time
import asyncio
import signal
import sys
import os

def signal_handler(sig, frame):
    print('Exiting...')
    sys.exit(0)

def main():
    signal.signal(signal.SIGINT, signal_handler)
    APP_CONFIG = os.environ.get('APP_CONFIG', r"C:\Users\dade\Desktop\AOAIBatchWorkingFork\aoai-batch-api-accelerator\config\app_config.json")
    utils = Utils()
    try:
        app_config_data = utils.read_json_data(APP_CONFIG)
        storage_config_data = utils.read_json_data(app_config_data["storage_config"])
        storage_account_name = storage_config_data["storage_account_name"]
        storage_account_key = storage_config_data["storage_account_key"]
        input_filesystem_system_name =  storage_config_data["input_filesystem_system_name"]
        error_filesystem_system_name = storage_config_data["error_filesystem_system_name"]
        processed_filesystem_system_name = storage_config_data["processed_filesystem_system_name"]
        input_directory = storage_config_data["input_directory"]
        output_directory = storage_config_data["output_directory"]
        error_directory = storage_config_data["error_directory"]
        aoai_config_data = utils.read_json_data(app_config_data["AOAI_config"])
        BATCH_PATH = "https://"+storage_account_name+".blob.core.windows.net/"+input_filesystem_system_name+"/"
        batch_size = int(app_config_data["batch_size"])
        count_tokens = int(app_config_data["count_tokens"])
        aoai_client = AOAIHandler(aoai_config_data)
        input_storage_handler = StorageHandler(storage_account_name, storage_account_key, input_filesystem_system_name)
        error_storage_handler = StorageHandler(storage_account_name, storage_account_key, error_filesystem_system_name)
        processed_storage_handler = StorageHandler(storage_account_name, storage_account_key, processed_filesystem_system_name)
        files = input_storage_handler.get_file_list(input_directory)
        input_directory_client = input_storage_handler.get_directory_client(input_directory)
        download_to_local = app_config_data["download_to_local"]
        local_download_path = None
        if download_to_local:
            local_download_path = app_config_data["local_download_path"]
        continuous_mode = app_config_data["continuous_mode"]
        azure_batch = AzureBatch(aoai_client, input_storage_handler, 
                                error_storage_handler, processed_storage_handler, BATCH_PATH, input_directory_client, 
                                local_download_path,output_directory, error_directory,count_tokens)
    except Exception as e:
        print(f"An error occurred while initializing the application, please check the configuration. \n\n\tException:\n\n\t\t{e}\n\n")
        return
    if continuous_mode:
        print("Running in continuous mode")
        while True:
            if len(files) > 0:
                asyncio.run(azure_batch.process_all_files(files, batch_size))
            else:
                print("No files found. Sleeping for 60 seconds")
                time.sleep(60) 
            files = input_storage_handler.get_file_list(input_directory)
    else:
        print("Running in on-demand mode")
        asyncio.run(azure_batch.process_all_files(files, batch_size))   

    #TODO: 1) Support blob storage
     


          
           
if __name__ == "__main__":
    main()