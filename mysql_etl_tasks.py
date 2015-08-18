"""
local database: luigid, table: original

read data from original, summarize and insert into summarized


what's working: insert data, copy data to new table
		taking summarized data and inserting into new table
what's needed: use luigi to create task flow
"""


import mysql.connector
import luigi
import mysql_target
from datetime import datetime

mysql = mysql.connector.connect(user='', password='', host='127.0.0.1', database='yaml_practice')

class MysqlTarget(luigi.Target):
    """
    Target for a resource in local mysql
    """

    def __init__(self, mysql_connection, key, value):

        self.connection = mysql_connection
        self.key = key
        self.value = value


    def create_marker_table(self):
        print "self.connection", self.connection

        connection = self.connection
        connection.connect()

        # print "connection", connection
        cursor = connection.cursor()
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS markers (
                    id              BIGINT(20)    NOT NULL AUTO_INCREMENT
                    , mark_key      VARCHAR(128)  NOT NULL
                    , mark_value    VARCHAR(128)  NOT NULL
                    , inserted      TIMESTAMP DEFAULT NOW()
                    , PRIMARY KEY (id)
                );
            """)
        self.connection.commit()
        self.connection.close()


    def exists(self):

        # create the marker table if it doesn't exist already
        self.create_marker_table()
        # connect to Unity
        connection = self.connection
        connection.connect()

        cursor = connection.cursor()
        # check that the key, value pair exists against the object of interest
        cursor.execute(
            """
                SELECT 1
                FROM markers
                WHERE
                    mark_key = '{key}'
                    AND mark_value = '{value}';
            """.format(key=self.key,
                       value=self.value))
        row = cursor.fetchone()
        connection.close()
        return row is not None


    def mark_table(self):

        # create the table if it doesn't already
        self.create_marker_table()
        # connect to local mysql
        connection = self.connection
        connection.connect()
        cursor = connection.cursor()
        # insert a mark against the table
        cursor.execute(
            """
                INSERT INTO markers (mark_key, mark_value)
                VALUES ('{key}', '{value}');
            """.format(key=self.key,
                       value=self.value)
            )
        connection.commit()
        # close the connection
        connection.close()



class StoryCount(luigi.Task):
    """"""
    # cnx = mysql.connector.connect(user='', password='',
    #                             host='127.0.0.1',
    #                             database='yaml_practice')
    # print "cnx = ", cnx
    # print "self.cnx =", self.cnx
    print "mysql =", mysql

    date = luigi.DateParameter()
    def output(self):
    	return MysqlTarget(mysql, 
                            'children_stories_count',
    						self.date)

    def run(self):
        connection = mysql
        connection.connect()

        cursor = connection.cursor()
        print "I am running"
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM children_stories
            WHERE style="Nursery Rhyme"
            """)
        row = cursor.fetchone()
        count = row[0]

        cursor.execute(
            """
            INSERT into 
            children_stories_count (quantity, updated)
            VALUES (%s, %s)
            """, (count, "2015-08-14"))
        connection.commit()
        connection.close()

        print "row =", row
        if row is None:
            return
        else:
            print "I made it here"
            mark = self.output()
            mark.mark_table()
            return



# def compute_and_insert_count_data(): 
# 	cnx = mysql.connector.connect(user='', password='',
#                               host='127.0.0.1',
#                               database='yaml_practice')

# 	cursor = cnx.cursor()
# 	cursor.execute(
#         """
#         SELECT COUNT(*)
#         FROM children_stories
#         WHERE style="Nursery Rhyme"
#         """)
# 	row = cursor.fetchone()
# 	count = row[0]

# 	cursor.execute(
#         """
#         INSERT into 
#         children_stories_count (quantity, updated)
#         VALUES (%s, %s)
#         """, (count, "2015-08-12"))
# 	cnx.commit()
# 	cnx.close()

# compute_and_insert_count_data()

if __name__ == "__main__":
    luigi.run()