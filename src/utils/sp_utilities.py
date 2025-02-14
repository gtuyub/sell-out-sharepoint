from azure.identity import ClientSecretCredential
from msgraph import GraphServiceClient
from msgraph.generated.models.site_collection_response import SiteCollectionResponse
from msgraph.generated.models.drive_collection_response import DriveCollectionResponse
from msgraph.generated.drives.drives_request_builder import DrivesRequestBuilder
from msgraph.generated.sites.sites_request_builder import SitesRequestBuilder
from msgraph.generated.models.field_value_set import FieldValueSet
from kiota_abstractions.base_request_configuration import RequestConfiguration
import asyncio
from typing import List
from io import BytesIO
import pandas as pd
from sqlalchemy import create_engine, Engine
from .exceptions import GraphRequestError, ExcelSheetsError, SQLEngineError, MissingColumnError, ColumnConversionError, EmptyExcelSheetError
from config.settings import CustomerConfig, ExcelFileMetadata



def init_graph_client(tenant_id : str , client_id : str , client_secret : str) -> GraphServiceClient:
    """
    Creates an instance of Microsoft Graph API client using client secret authentication.
    """

    credentials = ClientSecretCredential(tenant_id,client_id,client_secret)

    scopes = ['https://graph.microsoft.com/.default']

    graph_client = GraphServiceClient(credentials,scopes)

    return graph_client



async def get_site_by_query(graph_client : GraphServiceClient, query : str) -> SiteCollectionResponse:
    """
      Returns a SiteCollectionResponse object containing information about sharepoint sites on the tenant where the name of the site matches the query string.
    """

    query_params = SitesRequestBuilder.SitesRequestBuilderGetQueryParameters(
		search = f"{query}",
    )

    request_configuration = RequestConfiguration(
    query_parameters = query_params,
    )

    result = await graph_client.sites.get(request_configuration = request_configuration)

    return result



async def get_drives_from_site_id(graph_client : GraphServiceClient, site_id : str) -> DriveCollectionResponse:
    """
      Returns a DriveCollectionResponse object containing information about all drives (document libraries) within the site.
    """

    result = await graph_client.sites.by_site_id(site_id).drives.get()

    return result


async def get_lists_from_site_id(graph_client : GraphServiceClient, site_id : str):

    result = await graph_client.sites.by_site_id(site_id).lists.get()

    return result


async def get_files_from_drive_id(graph_client : GraphServiceClient, drive_id : str)-> bytes:

    result = await graph_client.drives.by_drive_id(drive_id).root.content.get()

    return result



async def get_drive_root_id(graph_client : GraphServiceClient , drive_id : str) -> str:
    """
    Returns the root folder id for a given drive object.
    """
    try:
        root = await graph_client.drives.by_drive_id(drive_id).root.get()
        if root:
            return root.id
    except Exception as e:
        raise GraphRequestError(f'Error al intentar obtener el directorio raíz de la biblioteca {drive_id} : {e}')


async def get_folders_from_drive(graph_client : GraphServiceClient, drive_id : str) -> List[List]:

    query_parameters = DrivesRequestBuilder.DrivesRequestBuilderGetQueryParameters(
        select=['id','name']
    )
    config = DrivesRequestBuilder.DrivesRequestBuilderGetRequestConfiguration(
        query_parameters=query_parameters
        )

    drive_root_id = await get_drive_root_id(graph_client, drive_id)
    try:
        response = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(drive_root_id).children.get(config)
        if response:
            data = [[drive_item.id, drive_item.name] for drive_item in response.value]
        while response is not None and response.odata_next_link is not None:
            response = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(drive_root_id).children.with_url(response.odata_next_link).get(config)
            data.extend([drive_item.id, drive_item.name] for drive_item in response.value)
        return data
    except Exception as e:
        raise GraphRequestError(F'Error al intentar obtener la lista de carpetas de la biblioteca {drive_id} : {e}')



async def get_files_from_folder(graph_client : GraphServiceClient, drive_id : str, folder_id : str) -> List[ExcelFileMetadata]:

    query_parameters = DrivesRequestBuilder.DrivesRequestBuilderGetQueryParameters(
        select = ['id', 'name'],
        expand = ['listItem']
                )
    config = DrivesRequestBuilder.DrivesRequestBuilderGetRequestConfiguration(
        query_parameters=query_parameters
    )

    try:
        response = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(folder_id).children.get(config)
        if response:
            items = []
            for drive_item in response.value:

                item_data = ExcelFileMetadata(drive_item.id,
                                              drive_item.name,
                                              drive_item.list_item.id,
                                              drive_item.list_item.fields.additional_data.get('Status')
                                              )

                items.append(item_data)

        while response is not None and response.odata_next_link is not None:
            response = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(folder_id).children.with_url(response.odata_next_link).get(config)
            for drive_item in response.value:

                item_data = ExcelFileMetadata(drive_item.id,
                                              drive_item.name,
                                              drive_item.list_item.id,
                                              drive_item.list_item.fields.additional_data.get('Status')
                                              )

                items.append(item_data)

        return items

    except Exception as e:

        raise GraphRequestError(f'Unable to retrieve files info from library : {drive_id}. folder : {folder_id}. Due to the following error: {e}')



async def get_drive_item_content(graph_client : GraphServiceClient, drive_id : str , item_id : str) -> bytes:
    try:
        response = await graph_client.drives.by_drive_id(drive_id).items.by_drive_item_id(item_id).content.get()
        return response
    except Exception as e:
        raise GraphRequestError(f'Error al intentar extraer el contenido del archivo {item_id} en la biblioteca {drive_id} : {e}')


async def get_file_content_into_df(graph_client : GraphServiceClient, drive_id : str, excel_file_metadata : ExcelFileMetadata) -> ExcelFileMetadata:

    try:
        file_name = excel_file_metadata.name
        file_id = excel_file_metadata.id

        print(f'downloading byte content for file {file_name}...')

        content = await get_drive_item_content(graph_client,drive_id,file_id)

        workbook = pd.ExcelFile(BytesIO(content), engine='openpyxl')

        if len(workbook.sheet_names) > 1:

            raise ExcelSheetsError(f'The excel file with name {file_name} contains multiple sheets; files to be processed must include only 1 sheet to prevent ambiguity.\n {workbook.sheet_names}')
        
        df = pd.read_excel(BytesIO(content),engine='openpyxl',dtype=str)

        excel_file_metadata.df = df

        print(f'se procesó con éxito la información del archivo {file_name}.')

    except Exception as e:

        excel_file_metadata.df = pd.DataFrame({})
        print(f'Error al intentar descargar el contenido binario del archivo {file_name}. {e}')


    return excel_file_metadata


async def get_unprocessed_files_content(graph_client : GraphServiceClient, drive_id : str, folder_id : str) -> List[ExcelFileMetadata]:

    print(f'Searching for new files to process on folder with ID : {folder_id}.')

    files_dict = await get_files_from_folder(graph_client,drive_id,folder_id)

    files_to_process = []

    for excel_file_metadata in files_dict:

        if excel_file_metadata.status in [None,'Error']:

            files_to_process.append(excel_file_metadata)

    if files_to_process:

        print(f'Se detectaron {len(files_to_process)} archivos nuevos para cargar.')
        print(f'archivos encontrados :')

        for excel_file_metadata in files_to_process:
            print(excel_file_metadata.name)

    else:

        print(f'No se encontraron archivos para cargar en la carpeta con ID : {folder_id}.')
        return


    files_to_process = await asyncio.gather(
        *[get_file_content_into_df(graph_client,drive_id,doc_dict) for doc_dict in files_to_process]
    )

    return files_to_process


async def update_list_item_status(graph_client : GraphServiceClient, site_id : str, list_id : str, list_item_id : str, status : str, error_description = None):

        if error_description:

            request_body = FieldValueSet(
                    additional_data = {
                        "Status": f'{status}',
                        "ErrorDescription": error_description  
                    }
                )

        else:
            request_body = FieldValueSet(
                additional_data = {
                    "Status" : f'{status}',
                    "ErrorDescription": ''
                }
            )

        result = await graph_client.sites.by_site_id(f'{site_id}').lists.by_list_id(f'{list_id}').items.by_list_item_id(f'{list_item_id}').fields.patch(request_body)




def validate_excel_file(excel_file : ExcelFileMetadata, config: CustomerConfig) -> pd.DataFrame:

    df = excel_file.df
    missing_columns = config.expected_columns - set(df.columns)

    if missing_columns:
        
        if df.empty:
            raise EmptyExcelSheetError(f'El archivo está vacío o contiene múltiples hojas por lo que no se pudo procesar.')
        
        raise MissingColumnError(f'El archivo {excel_file.name} no se pudo procesar, ya que no se encuentran las siguientes columnas: \n {missing_columns}')


    for col, dtype in config.dtypes_mapping.items():
        try:
            if dtype == 'int':
                df[col] = df[col].astype(int)

            if dtype == 'float':
                df[col] = df[col].astype(float)

            if dtype == 'str':
                df[col] = df[col].astype(str)

            if dtype == 'date':
                df[col] = pd.to_datetime(df[col],errors='raise')

        except Exception as e:
            raise ColumnConversionError(f'El archivo {excel_file.name} no se pudo procesar, debido a un error al intentar convertir la columna {col}.\n {e}.')


    df = df.rename(columns=config.columns_mapping)
    df = df[config.columns_mapping.values()]

    df['customer_code'] = config.customer_code

    return df



def create_db_engine(server : str, database : str, username : str, password : str) -> Engine:

    connection_url = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    try:
        engine = create_engine(connection_url)
        connection = engine.connect()
        connection.close()
        print(f'SQLAlchemy connection with context server : "{server}" database : "{database}" tested successfully.')
        return engine
    except Exception as e:
        raise SQLEngineError(f'Cannot create database engine with context:\n server : {server} \n database : {database}\n Error : {e}')

























