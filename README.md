### Daily Interface T1C

* Prepare environment

    ```
    $ brew install pytho3
    $ mkvirtualenv -p /usr/local/bin/python3 env3_mssql
    $ pip install -r setup.pip
    $ pip install git+https://github.com/pymssql/pymssql.git
    ```

* Code format with `yapf`

    yapf -i --style='{based_on_style: pep8, indent_width: 2}' <file.py>

* Airflow

* Reference

    - [Pymsql](http://gree2.github.io/python/setup/2017/04/19/python-instal-pymssql-on-mac)
    - [ODBC](https://github.com/mkleehammer/pyodbc/wiki/Connecting-to-SQL-Server-from-Mac-OSX)