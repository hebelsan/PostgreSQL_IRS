# PostgreSQL_IRS

This python code creates two database models for the task of information retrieval and compares them with Apache Solr and the tsvector of PostgreSQL.



##### Requirements:

- To install all necessary python libraries we use pipenv:
  `pipenv shell`
  `pipenv install`
- Furthermore a installation of PostgreSQL and a running Apache Solr on localhost is required
- To configure PostgreSQL use the scripts in the folder */PostgresConfig*



To run the main evaluation pipeline execute the script  */EvaluationPipeline/evaluation.py*.

