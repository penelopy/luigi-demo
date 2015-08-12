"""
local database: luigid, table: original

read data from original, summarize and insert into summarized


what's working: insert data, copy data to new table
what's not working: taking summarized data and inserting into new table
"""


import mysql.connector


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





