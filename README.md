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

To run the development version, rather than installing and calling the `w3act` command, you can use `python -m w3act.dbc.cmd`. For example, to see the CLI help:

    $ python -m w3act.dbc.cmd -h

The first step is to get a copy of the database downloaded. As of June 2022, this can be done like this:

    $ source ~/gitlab/ukwa-services-env/w3act/prod/w3act.env
    $ python -m w3act.dbc.cmd get-csv -H prod1.n45.wa.bl.uk -P 5432 -p $W3ACT_PSQL_PASSWORD

i.e. assuming the internal `ukwa-services-env` repository is available, so we can get the password for the database, and assuming the production database is running on that server and port.

Having downloaded the CSV into the default folder (`./w3act-db-csv`) the other commands that generate derivative data can be executed.

e.g. To populate an instance of the ukwa-ui-collections-solr index:

    $ python -m w3act.dbc.cmd -v update-collections-solr http://localhost:9021/solr/collections


Example of posting a document to a localhost version for testing:


    $ docker run --net host -ti ukwa/python-w3act w3act-api -u $USER -p $PW add-document 9022 20211003002015 https://www.amnesty.org/download/Documents/EUR2500882019ENGLISH.PDF https://www.amnesty.org/en/documents/eur25/0088/2019/en/


python -m w3act.dbc.cmd get-csv -H prod1 -p ${W3ACT_PSQL_PASSWORD} -d w3act-db-csv
python -m w3act.dbc.cmd update-collections-solr -v -d w3act-db-csv http://localhost:9021/solr/collections
