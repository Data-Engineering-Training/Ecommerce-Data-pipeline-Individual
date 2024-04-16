import config
from data_loaders import write_file_modification

def ingest_to_warehouse(df, table_name, file_path, last_index):
    try:
        df.write.jdbc(url=config.jdbc_url, table=table_name, mode="append", properties=config.mysql_properties)
        write_file_modification(file_path, last_index)
        
        return {"status": True, "error": None}
    
    except Exception as e:
        return {"status": False, "error": e}