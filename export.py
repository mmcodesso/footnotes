import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm

# Database connection parameters
db_config = {
    'username': 'footnotes',
    'password': 'footnotes',
    'host': 'localhost',
    'port': '5432',
    'database': 'footnotes'
}

# Create a database connection using SQLAlchemy
engine = create_engine(f"postgresql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}")

# SQL query to fetch data
query = """Select b."filing_accessionNumber", b."cikNumber", b.name, b.category, a.* from footnotes a
join fn32_06232023_10kq_cik b on a.textblock_id = b.textblock_id"""  # Replace with your query

# Chunk size
chunk_size = 10000  # Adjust this based on your memory constraints

# CSV file path
output_csv = "output_data.csv"

# Determine the number of rows for progress bar
total_rows = pd.read_sql(f"SELECT COUNT(*) FROM ({query}) as subquery", engine).iloc[0][0]
total_chunks = (total_rows // chunk_size) + 1

# Read data in chunks and write to CSV using tqdm for progress bar
with tqdm(total=total_chunks, desc="Exporting data", unit="chunk") as pbar:
    for chunk in pd.read_sql(query, engine, chunksize=chunk_size):
        # Append each chunk to CSV
        chunk.to_csv(output_csv, mode='a', header=not bool(pbar.n), index=False)
        pbar.update(1)

print("Data export complete.")
