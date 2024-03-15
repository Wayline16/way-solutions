from sqlalchemy import create_engine, text
import os

db_connection_string = os.environ['DB_CONNECTION_STRING']

engine = create_engine(db_connection_string, connect_args={
  "ssl": {
    "ssl_ca": "/etc/ssl/cert.pem"
  }
})

#Load: jobs from the database

def load_jobs_from_db():
  with engine.connect() as conn:
    result = conn.execute(
      text("SELECT * FROM jobs")
    )
    result_all = result.fetchall()
    jobs = []
    if result_all:
      column_names = result.keys()
      jobs = [dict(zip(column_names, row)) for row in result_all]
    else:
      print("No results found.")
  return jobs

def load_job_from_db(id):
  with engine.connect() as conn:
      query = text("SELECT * FROM jobs WHERE id = :id").params(id=id)
      result = conn.execute(query)
      result_all = result.fetchall()

      job = []
      if result_all:
          column_names = result.keys()
          job = [dict(zip(column_names, row)) for row in result_all]
      else:
          print("No results found.")

      return job[0]

def add_application_to_db(job_id, application):
  with engine.connect() as conn:
    query = text("INSERT INTO applications (job_id, full_name, email, linkedin_url, education, work_experience, resume_url) VALUES (:job_id, :full_name, :email, :linkedin_url, :education, :work_experience, :resume_url)").params(job_id=job_id,full_name=application['full_name'],email=application['email'],linkedin_url=application['linkedin_url'],education=application['education'],work_experience=application['work_experience'],resume_url=application['resume_url'])

    conn.execute(query)

#Load: technologies from database

def load_tech_from_db():
  with engine.connect() as conn:
    result = conn.execute(
      text("SELECT * FROM technologies")
    )
    result_all = result.fetchall()
    technologies = []
    if result_all:
      column_names = result.keys()
      technologies = [dict(zip(column_names, row)) for row in result_all]
    else:
      print("No results found.")
  return technologies


def load_tech_id_from_db(id):
  with engine.connect() as conn:
      query = text("SELECT * FROM technologies WHERE id = :id").params(id=id)
      result = conn.execute(query)
      result_all = result.fetchall()

      technology = []
      if result_all:
          column_names = result.keys()
          technology = [dict(zip(column_names, row)) for row in result_all]
      else:
          print("No results found.")

      return technology[0]

#Load: service from database

def load_services_from_db():
  with engine.connect() as conn:
    result = conn.execute(
      text("SELECT * FROM services")
    )
    result_all = result.fetchall()
    services = []
    if result_all:
      column_names = result.keys()
      services = [dict(zip(column_names, row)) for row in result_all]
    else:
      print("No results found.")
    return services