# Monitoring Dashboard App

This repository hosts the code for Class-Proxima's internal pipeline monitoring dashboard based on Plotyly Dash framework and written in Python. The dashboard fetches data about the deployed pipelines from our MLOps database (PostgresDB). 

## Downloading and running the app

First clone the repository. Then `cd` into the app directory and install its dependencies in a virtual environment in the following way:

```bash
python -m venv venv
source venv/bin/activate  # Windows: \venv\scripts\activate
pip install -r requirements.txt
```
Secondly, get the credentials of the MLOPs database and store them in config file named mlopsDB_config.yaml. The format of the config file will be as follows:

```yaml
username: postgres
password: XXXXXXXXXXX
hostname: yourDBhostname.amazon.com
database: dbname
port: 5432
aws_bucket: xyz-bucket
bucket_subpath: xyz-folder-name
```
Store this yaml file inside the app folder.

Now, you can run the app:
```bash
cd app
python app.py
```

## Cloning this whole repository

To clone this repository, run:
```
git clone https://github.com/plotly/dash-sample-apps
```
