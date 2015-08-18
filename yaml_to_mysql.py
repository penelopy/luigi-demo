import yaml
import mysql.connector

etl_yaml_files = ["hickory.yaml", "mice.yaml", "sheep.yaml"]

class YamlData():
	def __init__(self, name, style, description, quantity):
		self.name = name
		self.style = style
		self.description = description
		self.quantity = quantity

	def __repr__(self):
		return "%s(name=%r, style=%r, description=%r, quantity=%r)" % (
			self.__class__.__name__, self.name, self.style, self.description, self.quantity)

cnx = mysql.connector.connect(user='', password='',
                          host='127.0.0.1',
                          database='yaml_practice')

cursor = cnx.cursor()
cursor.execute("""
		CREATE TABLE IF NOT EXISTS children_stories (
			name VARCHAR(20), 
			style VARCHAR(20), 
			description VARCHAR(20), 
			quantity INT(10))
			""")
for etl in etl_yaml_files: 
	with open(etl, 'r') as ymlfile:
	    data = yaml.load(ymlfile)
	    yd =YamlData(name=data['name'], style=data['style'], description=data['description'], quantity=data['quantity'])

	cursor.execute("""
		INSERT INTO 
		children_stories (name, style, description, quantity)
		VALUES (%s, %s, %s, %s) 
		""", (yd.name, yd.style, yd.description, yd.quantity))
	cnx.commit()
cnx.close()










