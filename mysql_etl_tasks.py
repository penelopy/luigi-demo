import mysql.connector
import luigi
import time
import yaml
from datetime import datetime
import csv

class Db:
    @staticmethod
    def execute(sql):
        connection = mysql.connector.connect(user='root', password='', host='127.0.0.1', database='luigi_db')
        connection.connect()
        cursor = connection.cursor()
        cursor.execute(sql)
        row = cursor.fetchone()
        connection.commit()
        connection.close()
        return row

    @staticmethod
    def execute_yaml_file(filename):
        with open(filename, 'r') as ymlfile:
            data = yaml.load(ymlfile)
            return Db.execute(data['sql'])


class Seeds(luigi.Task):
    def output(self):
        return Markers(self.__class__.__name__, 'inserted_date')

    def run(self):
        yaml_seed_files = ["hubbard.yaml", "humpty.yaml", "rockabye.yaml", "littleboy.yaml", "crooked.yaml", "lamb.yaml", "twinkle.yaml", "winkle.yaml", "teepot.yaml", "georgie.yaml", "baa.yaml"]
        Db.execute(
            """
                CREATE TABLE IF NOT EXISTS stories (
                    id              INT   NOT NULL AUTO_INCREMENT
                    , title         VARCHAR(32) NOT NULL
                    , full_text     VARCHAR(128)  NOT NULL
                    , first_word    VARCHAR(20)  NOT NULL
                    , last_word     VARCHAR(20)  NOT NULL
                    , word_count    INT
                    , rating        INT
                     , PRIMARY KEY (id)
                 );
             """)
        for filename in yaml_seed_files:
            with open(filename, 'r') as ymlfile:
                data = yaml.load(ymlfile)
                # print data
                # return Db.execute(data['sql'])
            Db.execute(
            """
                INSERT INTO stories (title, full_text, first_word, last_word, word_count, rating)
                VALUES ('{title}','{full_text}', '{first_word}', '{last_word}', '{word_count}', '{rating}');
            """.format(title=data['title'],
                        full_text=data['full_text'],
                       first_word=data['first_word'],
                       last_word=data['last_word'],
                       word_count=data['word_count'],
                       rating=data['rating'] )
            )

#         with open('rhymes.csv', 'rU') as csvfile:
#             datareader = csv.reader(csvfile, delimiter=',', dialect=csv.excel_tab)
#             for fields in datareader:
#                 Db.execute(
#                     """
#                         INSERT INTO stories (full_text, first_word, last_word, word_count, rating)
#                         VALUES (full_text, first_word, last_word, word_count, rating);
#                     """.format(full_text=fields[0], first_word=fields[1], last_word=fields[2], word_count=fields[3], rating=fields[4]))

class Markers(luigi.Target):
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def create_marker_table(self):
        Db.execute(
            """
                CREATE TABLE IF NOT EXISTS markers (
                    id              BIGINT(20)    NOT NULL AUTO_INCREMENT
                    , mark_key      VARCHAR(128)  NOT NULL
                    , mark_value    VARCHAR(128)  NOT NULL
                    , inserted      TIMESTAMP DEFAULT NOW()
                    , PRIMARY KEY (id)
                );
            """)


    def exists(self):
        self.create_marker_table()
        row = Db.execute(
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
        self.create_marker_table()
        Db.execute(
            """
                INSERT INTO markers (mark_key, mark_value)
                VALUES ('{key}', '{value}');
            """.format(key=self.key,
                       value=self.value)
            )
         

class MakeDateColumnOnChildrenStories(luigi.Task):
    def output(self):
        return Markers('make_new_column', 'date_column_in_children_stories')

    def run(self):
        Db.execute(
            """
            ALTER TABLE
            children_stories_count
            ADD date datetime
            """
        )
        mark = self.output()
        mark.mark_table()

# class Story(luigi.Task):
#     date = luigi.DateParameter()

#     def requires(self):
#         return MakeDateColumnOnChildrenStories()

#     def output(self):
#         return Markers('children_story', self.date)

#     # def read_sql(self, filename):
#     #     with open(filename, 'r') as ymlfile:
#     #         data = yaml.load(ymlfile)
#     #         self.output().execute(data['sql'])  

#     def run(self):
#         s = Db.execute_yaml_file("todays_top_story.yaml") #FIX tried using self.output().read_sql but...
#         mark = self.output()
#         mark.mark_table()

  
class StoryCount(luigi.Task):
    date_interval = luigi.DateIntervalParameter()
    yaml_files = ["story_count.yaml", "todays_top_story.yaml"]

    def requires(self):
        # return [Story(date) for date in self.date_interval]
        return Seeds()

    def output(self):
    	return Markers(self.__class__.__name__, self.date_interval)

    def run(self):
        row = Db.execute_yaml_file(self.yaml_files[0])
        mark = self.output()
        mark.mark_table()
        return
                


# class YamlPoweredTask(luigi.Task):
#     file_name = ""
#     yaml = dict()#somehow_read_yaml()

#     def requires(self):
#         return [YamlPoweredTask(file_name=dependency) for dependency in self.yaml['dependencies']]

#     def run(self):
#         execute(self.yaml['sql'])

if __name__ == "__main__":
    luigi.run()