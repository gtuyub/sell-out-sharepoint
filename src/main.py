from config.settings import Config, CustomerConfig
from utils.sp_utilities import init_graph_client, create_db_engine, get_unprocessed_files_content, validate_excel_file, update_list_item_status
import pandas as pd
import asyncio


async def main(customer_code : str) -> None:

    config = Config.load_from_env()

    graph_client = init_graph_client(config.api.tenant_id,
                                     config.api.client_id,
                                     config.api.client_secret)
    
    sql_engine = create_db_engine(config.db.server,
                                  config.db.database,
                                  config.db.username,
                                  config.db.password)
    
    customer_config = CustomerConfig.load_from_yaml(customer_code)


    files_to_process = await get_unprocessed_files_content(graph_client,
                                                                customer_config.drive_id,
                                                                customer_config.folder_id)
    
    for excel_file in files_to_process:

        try:
            excel_file.df = validate_excel_file(excel_file,customer_config)

            excel_file.df.to_sql(name=customer_config.table,
                                 con=sql_engine,
                                 if_exists='append',
                                 index=False)
                        
        except Exception as e:

            print(f'Se omitió el archivo: {excel_file.name} por el siguiente error : \n {e}')
            error_description = str(e)
            excel_file.df = pd.DataFrame({})

        finally:

            if excel_file.df.empty:

                await update_list_item_status(graph_client,
                                              customer_config.site_id,
                                              customer_config.list_id,
                                              excel_file.list_item_id,
                                              status='Error',
                                              error_description=error_description)
            else:

                await update_list_item_status(graph_client,
                                              customer_config.site_id,
                                              customer_config.list_id,
                                              excel_file.list_item_id,
                                              status='Cargado')






if __name__ == '__main__':

    asyncio.run(main(customer_code = 'FN000354'))
            



