from flask import Flask, render_template, jsonify

app = Flask(__name__)

JOBS = [
  {
    'id' : 1,
    'title' : 'Data Analyst',
    'location' : 'Bengaluru, India',
    'salary' : 'Rs. 10,00,000'

  },
  {
    'id' : 2,
    'title' : 'Data Scientist',
    'location' : 'Dehli, India',
    'salary' : 'Rs. 15,00,000'

  },
{
  'id' : 3,
  'title' : 'Front Engineer',
  'location' : 'Remote',

},
  {
    'id' : 4,
    'title' : 'Backend Engineer',
    'location' : 'San Francisco, USA',
    'salary' : '$120, 000'

  }

]

@app.route("/")
def hello_wayline():
  return render_template('home.html', jobs=JOBS, company_name='Wayline')

@app.route("/api/jobs")
def list_jobs():
  return jsonify(JOBS)

if __name__ == "__main__":
  app.run(host='0.0.0.0', debug=True)

