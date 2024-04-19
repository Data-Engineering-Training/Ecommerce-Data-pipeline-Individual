import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

DB_NAME=os.environ.get('DB_NAME')
DB_USER=os.environ.get('DB_USER')
DB_PASSWORD=os.environ.get('DB_PASSWORD')
DB_HOST=os.environ.get('DB_HOST')
DB_DIALECT=os.environ.get('DB_DIALECT')


# Database Configurations

jdbc_url = f"jdbc:mysql://{DB_HOST}/{DB_NAME}"
mysql_properties = {
            "user": DB_USER,
            "password": DB_PASSWORD,
            "driver": "com.mysql.jdbc.Driver"
        }