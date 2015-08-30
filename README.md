## What is Luigi?  
Luigi is an open-source framework for building data pipelines, and managing workflows. It handles dependency management and helps with scheduling complex batch jobs.
See https://github.com/spotify/luigi

Scheduling and running ETLs is a great use case for Luigi. 

##Installation 
**Mysql Setup**  
 * Download and install mysql.connector https://dev.mysql.com/downloads/connector/python/  
 * `pip install mysql-connector-python --allow-external mysql-connector-python`  
 * `mysql.server start`  
 *  Open local mysql `mysql 
 * `create database `  
 * `create table children_stories_count  (name VARCHAR(20), quantity INT(10), updated DATE);`

**PyYaml Installation**
  * http://pyyaml.org/download/pyyaml/PyYAML-3.11.tar.gz  

**Luigi Installation**  
  * `pip install luigi`
  
## Luigi Demo
This demo creates and seeds a local mysql database by reading and parsing multiples yaml files.   
`python mysql_etl_tasks.py Seeds`  

Start the first ETL  
`python mysql_etl_tasks.py StoryCount 2015-08-20-2015-08-30`  

The class 'StoryCount' requires an instance of the class 'Markers' which will generate a mark on the Marker table every time 'StoryCount' is run

###To Do
