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

class MysqlTarget(luigi.Target):
    """
    Target for a resource in local mysql
    """

    def __init__(self, key, value):

        self.connection = mysql.connector.connect(user='', password='', host='127.0.0.1', database='yaml_practice')
        self.key = key
        self.value = value


    def create_marker_table(self):
        print "self.connection", self.connection

        # print "connection", connection
        cursor = self.connection.cursor()
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
        row = self.execute(
            """
                SELECT 1
                FROM markers
                WHERE
                    mark_key = '{key}'
                    AND mark_value = '{value}';
            """.format(key=self.key,
                       value=self.value))
        return row is not None


    def mark_table(self):

        # create the table if it doesn't already
        self.create_marker_table()
        # connect to local mysql
        # insert a mark against the table
        self.execute(
            """
                INSERT INTO markers (mark_key, mark_value)
                VALUES ('{key}', '{value}');
            """.format(key=self.key,
                       value=self.value)
            )
        
    def execute(self, sql):
        self.connection.connect()
        cursor = self.connection.cursor()
        cursor.execute(sql)
        row = cursor.fetchone()
        self.connection.commit()
        # close the connection
        self.connection.close()
        return row

        
class StoryCount(luigi.Task):
    """"""
    date = luigi.DateParameter()

    def output(self):
    	return MysqlTarget('children_stories_count', self.date)

    def run(self):
        row = self.output().execute(
            """
            SELECT COUNT(*)
            FROM children_stories
            WHERE style="Nursery Rhyme"
            """)
        count = row[0]

        self.output().execute(
            """
            INSERT into 
            children_stories_count (quantity, updated)
            VALUES ({quantity}, '{updated}')
            """.format(quantity=count, updated="2015-08-14"))
        
        if row is None:
            return
        else:
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