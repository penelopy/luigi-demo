"""
These methods work - just not needed in current program
"""

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


# create_data()


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


#Incomplete
# class CheckForNullQuantity(luigi.Task):
#     def output(self):
#         return Markers('check_null', 'date_checked')

#     def run(self):
#         self.output().execute(
#             """ """)


























