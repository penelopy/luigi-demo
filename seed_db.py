import csv
from mysql_etl_tasks import Db


def create_table():
    Db.execute(
        """
            CREATE TABLE IF NOT EXISTS stories (
                id              INT   NOT NULL AUTO_INCREMENT
                , full_text     VARCHAR(128)  NOT NULL
                , first_word    VARCHAR(20)  NOT NULL
                , last_word		VARCHAR(20)  NOT NULL
                , word_count	INT
                , rating		INT
                , inserted      TIMESTAMP DEFAULT NOW()
                , PRIMARY KEY (id)
            );
        """)
create_table()
with open('rhymes.csv', 'rU') as csvfile:
    datareader = csv.reader(csvfile, delimiter=',', dialect=csv.excel_tab)
    for fields in datareader:
        Db.execute(
            """
                INSERT INTO stories (full_text, first_word, last_word, word_count, rating)
                VALUES (fields[0], fields[1], fields[2], fields[3], fileds[4]);
            """)