from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
    FileSystemClient
)
import json
class StorageHandler:
    def __init__(self, storage_account_name, storage_account_key, file_system_name=None):
        self.storage_account_name = storage_account_name
        self.storage_account_key = storage_account_key
        self.service_client = self.get_service_client_account_key(storage_account_name, storage_account_key)
        if file_system_name is not None:
            self.file_system_client = self.get_file_system_client(file_system_name)
        else:
            self.file_system_client = None
    def get_directories(self,path):
        paths = self.file_system_client.get_paths(path=path)
        return_paths = []
        for current_path in paths:
            if current_path.is_directory:
                return_paths.append(current_path.name)
        #No subdirectories found, return the current directory
        if len(return_paths) == 0:
            return_paths.append(path)
        return return_paths
    def write_content_to_directory(self, file_content, directory_name, output_filename):
        write_result = False       
        dir_exists = self.check_directory_exists(directory_name)
        if(dir_exists):
            error_directory_client = self.get_directory_client(directory_name)
        else:
            error_directory_client = self.create_directory(directory_name)
        result_file_content_status = self.write_json_to_storage(output_filename,file_content,error_directory_client)
        if result_file_content_status:
            write_result = True
            print(f"File {output_filename} written to storage directory.")
        else:
            print(f"Error writing file {output_filename} to error directory.")
        return write_result

    def write_json_to_storage(self,output_name,output_data,directory_client):
        return_code = True
        try:
            file_client = directory_client.get_file_client(output_name)
            file_client.upload_data(output_data, overwrite=True)
        except Exception as e:
            return_code = False
        finally:
            return return_code
    def check_directory_exists(self,directory_name):
        return_status = False
        try:
            directory_client = self.file_system_client.get_directory_client(directory_name)
            if directory_client.exists():
                return_status = True
            else:
                return_status = False
        except Exception as e:
            return_status = False
        return return_status   
    def create_directory(self, directory_name: str) -> DataLakeDirectoryClient:
        directory_client = self.file_system_client.create_directory(directory_name)
        return directory_client
    
    def get_directory_client(self, directory_name: str) -> DataLakeDirectoryClient:
        directory_client = self.file_system_client.get_directory_client(directory_name)
        return directory_client
    
    def get_file_list(self, path: str) -> list:
        file_list = [] 
        paths = self.file_system_client.get_paths(path=path)
        for path in paths:
            if not path.is_directory:
                file_list.append(path.name)
        return file_list
    def get_file_data(self, file_name,directory_client):
        file_client = directory_client.get_file_client(file_name)
        download = file_client.download_file()
        return download.readall()
    def delete_file_data(self, file_name,directory_client):
        return_status = True
        try:
            file_client = directory_client.get_file_client(file_name)
            file_client.delete_file()
        except Exception as e:
            return_status = False
        return return_status
    def save_file_to_local(self, file_name, directory_client, local_path):
        file_client = directory_client.get_file_client(file_name)
        download = file_client.download_file()
        data = download.readall()
        try:
            with open(local_path, "wb") as file:
                file.write(data)
            print(f"File {file_name} saved to local path {local_path}")
        except Exception as e:
            print(f"An error occurred while saving file {file_name} to local path {local_path}: {e}")
        return data

    def get_file_system_client(self, file_system_name: str) -> FileSystemClient:
        file_system_client = self.service_client.get_file_system_client(file_system_name)
        return file_system_client

    def get_service_client_account_key(self, account_name, account_key) -> DataLakeServiceClient:
        account_url = f"https://{account_name}.dfs.core.windows.net"
        service_client = DataLakeServiceClient(account_url, credential=account_key)

        return service_client

