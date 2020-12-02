python-w3act
==============

`python-w3act` is a Python package for handling interactions with the [w3act](https://github.com/ukwa/w3act/) service.

It encapsulates common operations, focussing on extracting data from W3ACT.

The w3act command is the main interface, which is based around downloading a CSV version of the Java W3ACT PostgreSQL database, then analysing and extracting data from that.

Deployment is via Docker container, and scripts in the ukwa-services repository.

## Usage when developing the code

When developing this code, it can be used as follows. First set up a suitable Python 3 virtualenv

    $ virtualenv -p python3 venv
    $ source venv/bin/activate
    $ # **TODO ADD ANY OS LIBRARIES NEEDED**
    $ pip install -r requirements.txt

To run the development version, rather than installing and calling the `w3act` command, you can use `python ./w3act/dbc/cmd.py`. For example, to see the CLI help:

    $ PYTHONPATH=. python ./w3act/dbc/cmd.py -h

The first step is to get a copy of the database downloaded. As of late 2020, this can be done like this:

    $ source ~/gitlab/ukwa-services-env/w3act/prod/w3act.env
    $ PYTHONPATH=. python ./w3act/dbc/cmd.py get-csv -H 192.168.45.60 -P 5434 -p $W3ACT_PSQL_PASSWORD

i.e. assuming the internal `ukwa-services-env` repository is available, so we can get the password for the database, and assuming the production database is running on that server and port.

Having downloaded the CSV into the default folder (`./w3act-db-csv`) the other commands that generate derivative data can be executed.

e.g. To populate an instance of the ukwa-ui-collections-solr index:

    $ PYTHONPATH=. python ./w3act/dbc/cmd.py -v update-collections-solr http://localhost:9021/solr/collections

