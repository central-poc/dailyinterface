### Airflow Batch Scheduler

* Prepare environment

    ```
	  $ brew uninstall --force freetds
    $ brew install freetds@0.91
    $ brew link --force freetds@0.91
    $ brew install python3
    $ python3 -m venv ~/.venv/airflow
	  $ source ~/.venv/airflow/bin/activate
    $ pip install -r setup.pip
    ```

* Code format with `yapf`

    yapf -i --style='{based_on_style: pep8, indent_width: 2}' <file.py>

* Airflow

* Reference

    - [Pymsql](http://gree2.github.io/python/setup/2017/04/19/python-instal-pymssql-on-mac)
    - [ODBC](https://github.com/mkleehammer/pyodbc/wiki/Connecting-to-SQL-Server-from-Mac-OSX)
    - [SQLAlchemy](http://thelaziestprogrammer.com/sharrington/databases/connecting-to-sql-server-with-sqlalchemy)