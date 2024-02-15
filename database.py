from sqlalchemy import create_engine, text
import os

db_connection_string = os.environ['DB_CONNECTION_STRING']

engine = create_engine(db_connection_string, connect_args={
  "ssl": {
    "ssl_ca": "/etc/ssl/cert.pem"
  }
})

def load_jobs_from_db():
  with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM jobs"))
    result_all = result.fetchall()
    jobs = []
    if result_all:
      column_names = result.keys()
      jobs = [dict(zip(column_names, row)) for row in result_all]
    else:
      print("No results found.")
  return jobs