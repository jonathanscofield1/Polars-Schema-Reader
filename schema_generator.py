import polars as pl
import os
import json

# Class to read schema information for large files into a new folder
class Schema:
    def __init__(self, folder_path):
        self.folder_path = folder_path
        self.meta_folder = self.folder_path + '/METADATA'
        self.sample_folder = self.meta_folder + '/SAMPLE'
        self.schema_folder = self.meta_folder + '/SCHEMA'
        
    def list_csv_files(self): # Get all CSV files in directory
        return [file for file in os.listdir(self.folder_path) if file.endswith('.csv')] 
    
    def make_folder(self, folder):
        try:
            os.mkdir(folder) # Create new folder
        except:
            os.rmdir(folder) # Remove old folder
            os.mkdir(folder) # Create new folder
            
    def make_meta_folders(self): # Create directories to store files
        [self.make_folder(i) for i in [self.meta_folder, self.sample_folder, self.schema_folder]]
            
    def get_schemas(self): # Analyze all files in folders and get metadata info
        self.make_meta_folders()
        schemas = dict()
        for file in self.list_csv_files():
            file_path = self.folder_path + '/' + file
            new_file_path = self.sample_folder + '/' + file.replace('.csv', '_SAMPLE.csv') # Write sample to this path
            sample_data = pl.read_csv(file_path, n_rows = 10, ignore_errors = True, encoding = 'utf8-lossy') # Get 1st 10 rows
            sample_data.write_csv(new_file_path) 
            schema = pl.scan_csv(file_path, infer_schema_length = 5000, try_parse_dates = True, ignore_errors = True, encoding = 'utf8-lossy').schema
            for i in schema.keys():
                schema[i] = str(schema[i]) # Convert Polars class to string
            schemas[file] = schema # Add to schemas dictionary
            
        with open(self.schema_folder + '/schemas.json','w') as f: # Write schemas to json
            json.dump(schemas, f, indent = 4)
            return schemas 
        

folder = Schema(input('Input folder path:\n'))


folder.get_schemas()

