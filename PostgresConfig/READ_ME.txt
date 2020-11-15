install psycopg2 in python3
# sudo apt-get install postgresql libpq-dev postgresql-client postgresql-client-common
# sudo apt-get update -y
# sudo apt-get install -y python3-psycopg2

The script set_postgres_paras.py sets parameters in the ostgresql.config file of your system.
Therefore it has to be executed as superuser:
# sudo -u postgres python set_postgres_paras.py

The changes will have effect after restarting the postgresql server:
# /etc/init.d/postgresql restart

To create nesessary databases you have to use pipenv and run the script "create_databases.py" without superuser

To vaccuum all databases run 
# sudo -u postgres python vacuum.py
