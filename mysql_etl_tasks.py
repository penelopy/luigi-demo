import mysql.connector
import luigi
import time
import yaml
from datetime import datetime

class Db:
    @staticmethod
    def execute(sql):
        connection = mysql.connector.connect(user='', password='', host='127.0.0.1', database='yaml_practice')
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

    # def read_sql(self, filename):
    #     with open(filename, 'r') as ymlfile:
    #         data = yaml.load(ymlfile)
    #         self.output().execute(data['sql'])  

    def run(self):
        s = Db.execute_yaml_file("todays_top_story.yaml") #FIX tried using self.output().read_sql but...
        mark = self.output()
        mark.mark_table()

  
class StoryCount(luigi.Task):
    date_interval = luigi.DateIntervalParameter()

    def requires(self):
        return [Story(date) for date in self.date_interval]

    def output(self):
    	return Markers('children_stories_count', self.date_interval)

    def run(self):
        row = Db.execute_yaml_file("story_count.yaml")
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