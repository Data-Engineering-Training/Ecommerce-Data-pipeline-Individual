import config

def ingest_to_warehouse(df, table_name):
    try:
        # Write DataFrame to MySQL database
        jdbc_url = f"jdbc:mysql://{config.DB_HOST}:3306/{config.DB_NAME}"
        mysql_properties = {
            "user": config.DB_USER,
            "password": config.DB_PASSWORD,
            "driver": "com.mysql.jdbc.Driver"
        }

        df.write.jdbc(url=jdbc_url, table=table_name, mode="append", properties=mysql_properties)
        return {"status": True, "error": None}
    
    except Exception as e:
        return {"status": False, "error": e}