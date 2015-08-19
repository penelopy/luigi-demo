import mysql.connector
import luigi
import time
import yaml
from datetime import datetime

class Markers(luigi.Target):
    def __init__(self, key, value):
        self.connection = mysql.connector.connect(user='', password='', host='127.0.0.1', database='yaml_practice')
        self.key = key
        self.value = value

    def create_marker_table(self):
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
        self.create_marker_table()
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
        self.create_marker_table()
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
        self.connection.close()
        return row

class MakeDateColumnOnChildrenStories(luigi.Task):
    def output(self):
        return Markers('make_new_column', 'date_column_in_children_stories')

    def run(self):
        self.output().execute(
            """
            ALTER TABLE
            children_stories
            ADD date datetime
            """
        )
        mark = self.output()
        mark.mark_table()

class Story(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return MakeDateColumnOnChildrenStories()

    def output(self):
        return Markers('children_story', self.date)

    def run(self):
        row = self.output().execute(
            """
            INSERT INTO
            children_stories
            (name, date)
            VALUES
            ("Hickory Dickory Dock", '{date}')
            """.format(date=self.date))

        mark = self.output()
        mark.mark_table()
        

class StoryCount(luigi.Task):
    """"""
    date_interval = luigi.DateIntervalParameter()

    def requires(self):
        return [Story(date) for date in self.date_interval]

    def output(self):
    	return Markers('children_stories_count', self.date_interval)

    def run(self):
        self.output().execute(self.read_sql("story_count.yaml"))
        # count = row[0]

        # self.output().execute(
        #     """
        #     INSERT into 
        #     children_stories_count (quantity, updated)
        #     VALUES ({quantity}, '{updated}')
        #     """.format(quantity=count, updated="2015-08-14"))
        

        mark = self.output()
        mark.mark_table()
        return           

    def read_sql(self, filename):
        with open(filename, 'r') as ymlfile:
            data = yaml.load(ymlfile)
            self.output().execute(data['sql'])
                


# class YamlPoweredTask(luigi.Task):
#     file_name = ""
#     yaml = dict()#somehow_read_yaml()

#     def requires(self):
#         return [YamlPoweredTask(file_name=dependency) for dependency in self.yaml['dependencies']]

#     def run(self):
#         execute(self.yaml['sql'])

if __name__ == "__main__":
    luigi.run()