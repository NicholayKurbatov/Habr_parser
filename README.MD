# Habr Parser
This is a test project that allows you to asynchronously parse the posts of hubs in Russian habr (the number of simultaneous url queries is selected by the user). The interval of parsing of the hubs of interest occurs after a fixed time set by the user. All available hubs are in the database. As a DBMS -- sqlite.

## Start running
An example of a test run is found in the file -- `main.py`.

A set of parameters set by the user:
1. hubs parsing period -- `RUN_INTERVAL`
2. number of simultaneous url queries -- `SIM_QUERY`
3. database name -- `DATABASE`
4. name of table in DB with posts -- `POST_TABLE`
5. name of table in DB with habs -- `HAB_TABLE`
6. list of habs, which should be parsed -- `HABS_PARSING`

SQLite database and log file are located in folder `current_files`


