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

# class MysqlTarget(luigi.Target):
#     """
#     Target for a resource in local mysql
#     """

#     def __init__(self, mysql_connection, schema, table, key, value):

#         self.connection = mysql_connection
#         self.schema = schema
#         self.table = table
#         self.key = key
#         self.value = value


#     def create_marker_table(self):

#         connection = self.connection.connect()
#         cursor = connection.cursor()
#         cursor.execute(
#             """
#                 CREATE TABLE IF NOT EXISTS markers (
#                     id              BIGINT(20)    NOT NULL AUTO_INCREMENT
#                     , schema_name   VARCHAR(128)  NOT NULL
#                     , table_name    VARCHAR(128)  NOT NULL
#                     , mark_key      VARCHAR(128)  NOT NULL
#                     , mark_value    VARCHAR(128)  NOT NULL
#                     , inserted      TIMESTAMP DEFAULT NOW()
#                     , PRIMARY KEY (id)
#                     , KEY idx_table_mark (schema_name, table_name, mark_key)
#                 );
#             """)
#         connection.commit()
#         connection.close()


#     def exists(self):

#         # create the marker table if it doesn't exist already
#         self.create_marker_table()
#         # connect to Unity
#         connection = self.connection.connect()
#         cursor = connection.cursor()
#         # check that the key, value pair exists against the object of interest
#         cursor.execute(
#             """
#                 SELECT 1
#                 FROM markers
#                 WHERE
#                     schema_name = '{schema}'
#                     AND table_name = '{table}'
#                     AND mark_key = '{key}'
#                     AND mark_value = '{value}';
#             """.format(schema=self.schema,
#                        table=self.table,
#                        key=self.key,
#                        value=self.value))
#         row = cursor.fetchone()
#         connection.close()
#         return row is not None


#     def mark_table(self):

#         # create the table if it doesn't already
#         self.create_marker_table()
#         # connect to local mysql
#         connection = self.connection.connect()
#         cursor = connection.cursor()
#         # insert a mark against the table
#         cursor.execute(
#             """
#                 INSERT INTO markers (schema_name, table_name, mark_key, mark_value)
#                 VALUES ('{schema}', '{table}', '{key}', '{value}');
#             """.format(schema=self.schema,
#                        table=self.table,
#                        key=self.key,
#                        value=self.value)
#             )
#         connection.commit()
#         # close the connection
#         connection.close()



class FirstTask(luigi.Task):
    """"""
    def output(self):
    	return MysqlTarget(mysql_connection, 
    						
    						'summaries', 
    						'python_count', 
    						str(datetime.utcnow().date()))
    	database, user, password, table, update_id):
        # return square_luigi.UnityTarget(unity
        #                                 , 'batcave_production_proxy'
        #                                 , 'payments'
        #                                 , 'payments_today'
        #                                 , str(datetime.utcnow().date()))

    def run(self):
		cnx = mysql.connector.connect(user='', password='',
                              host='127.0.0.1',
                              database='luigid')

		cursor = cnx.cursor()
        cursor.execute(
            """
            SELECT 1
            FROM batcave_production_proxy.payments
            WHERE created_at >= DATE(NOW())
            LIMIT 1
            """)
        row = cursor.fetchone()
        connection.close()

        if row is None:
            return
        else:
            mark = square_luigi.UnityTarget(unity
                                        , 'batcave_production_proxy'
                                        , 'payments'
                                        , 'payments_today'
                                        , str(datetime.utcnow().date()))
            mark.mark_table()
            return

def create_data(): 
	cnx = mysql.connector.connect(user='', password='',
                              host='127.0.0.1',
                              database='luigid')

	cursor = cnx.cursor()
	# cursor.execute(
 #        """
 #        SELECT *
 #        FROM original
 #        WHERE subject="Python"
 #        """)
	# rows = cursor.fetchall() # This works

	cursor.execute(
        """
        INSERT into
        subject_summary
        VALUES ("Python", 13, '2015-08-12')
        """)
	cnx.commit()
	cnx.close()


create_data()

def compute_and_insert_count_data(): 
	cnx = mysql.connector.connect(user='', password='',
                              host='127.0.0.1',
                              database='luigid')

	cursor = cnx.cursor()
	cursor.execute(
        """
        SELECT COUNT(*)
        FROM original
        WHERE subject="Python"
        """)
	row = cursor.fetchone()
	count = row[0]

	cursor.execute(
        """
        INSERT into 
        subject_summary (subject, quantity, updated)
        VALUES (%s, %s, %s)
        """, ("Python", count, "2015-08-12"))
	cnx.commit()
	cnx.close()

# compute_and_insert_count_data()


def copy_data_to_diff_table(): #reads data from one table and writes some fields (not all) to a new table
	cnx = mysql.connector.connect(user='', password='',
                              host='127.0.0.1',
                              database='luigid')

	cursor = cnx.cursor()
	count = extract_data()

	cursor.execute("""
        INSERT INTO book_topic_summary (
            label
            , quantity
            , updated
        )
        SELECT
            label
            , quantity
            , updated
        FROM original
        ;
        """)
	cnx.commit()
	cnx.close()

# copy_data_to_diff_table()