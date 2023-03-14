import pandas as pd
from sqlalchemy import create_engine
from pymongo import MongoClient

# Connect to the PostgreSQL database using SQLAlchemy
engine = create_engine('postgresql://postgres:jidgnesh@localhost/flights')

# Fetch data from the flights table
df = pd.read_sql_query("SELECT * FROM flights", engine)

# Connect to the MongoDB database
client = MongoClient('localhost', 27017)
db = client.flights

# Insert data into the MongoDB database
for index, row in df.iterrows():
    db.flights.insert_one(row.to_dict())

# Close the connections
engine.dispose()
client.close()
