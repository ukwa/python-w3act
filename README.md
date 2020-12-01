python-w3act
==============

`python-w3act` is a Python package for handling interactions with the [w3act](https://github.com/ukwa/w3act/) service.

It encapsulates basic API operations, focussing on extracting data from W3ACT.

The w3act command is the main interface, but the library can be used from tasks.


python ./w3act/dbc/cmd.py get-csv -H 192.168.45.60 -P 5434 -p $W3ACT_PSQL_PASSWORD
source /home/anj/gitlab/ukwa-services-env/w3act/prod/w3act.env
