from dataclasses import dataclass
from typing import Optional
import os
from pathlib import Path
from dotenv import load_dotenv
import yaml
from pandas import DataFrame
from typing import Dict


@dataclass
class CustomerConfig:

    customer_code : str
    table : str
    site_id : str
    list_id : str
    drive_id : str
    folder_id : str
    expected_columns : set
    columns_mapping : Dict[str,str]
    dtypes_mapping : Dict[str,str]

    @classmethod
    def load_from_yaml(cls, customer_code :str) -> 'CustomerConfig':

        with open(f'src\config\schemas\{customer_code}.yaml','r') as file:

            customer_data = yaml.safe_load(file)

        site_id = customer_data['source']['site_id']
        drive_id = customer_data['source']['library_id']
        folder_id = customer_data['source']['folder_id']
        list_id = customer_data['source']['list_id']
        table = customer_data['target']['table']
        expected_columns = set(customer_data['columns'].keys())
        columns_mapping = {col : d['sql_field'] for col, d in customer_data['columns'].items()}
        dtypes_mapping = {col : d['type'] for col, d in customer_data['columns'].items()}

        return cls(customer_code,table,site_id,list_id,drive_id,folder_id,expected_columns,columns_mapping,dtypes_mapping)



@dataclass
class GraphAPIConfig:
    tenant_id : str
    client_id : str
    client_secret : str

@dataclass
class DatabaseConfig:
    username : str
    password : str
    server : str
    database : str

@dataclass
class Config:
    api : GraphAPIConfig
    db : DatabaseConfig

    @classmethod
    def load_from_env(cls,env_path: Optional[Path] = None, override : bool = False) -> 'Config':

        load_dotenv(dotenv_path=env_path,override=override)
        
        api_config = GraphAPIConfig(
            
            tenant_id = os.getenv('TENANT_ID'),
            client_id = os.getenv('CLIENT_ID'),
            client_secret = os.getenv('CLIENT_SECRET')
        )

        db_config = DatabaseConfig(

            username = os.getenv('SQL_USERNAME'),
            password = os.getenv('SQL_PASSWORD'),
            server = os.getenv('SERVER'),
            database = os.getenv('DATABASE')
        )

        return cls(api=api_config, db=db_config)
    


@dataclass
class ExcelFileMetadata:

    id : str
    name : str
    list_item_id : str
    status : str
    df : Optional[DataFrame] = None