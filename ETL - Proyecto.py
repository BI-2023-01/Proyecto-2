import psycopg2
import csv

conn = psycopg2.connect(
    host="localhost",
    port="5433",
    database="asmaBI",
    user="postgres",
    password="postgres"
)

cursor = conn.cursor()

cursor.execute("DROP SCHEMA public CASCADE;")
cursor.execute("CREATE SCHEMA public;")

cursor.execute("create table localidades (localidad char(25));")
cursor.execute("create table fecha (ano smallint);")

localidades = []

cursor.execute("INSERT INTO fecha (ano) VALUES (2017)")
cursor.execute("INSERT INTO fecha (ano) VALUES (2021)")

def agregarLocalidad(localidad):
    if localidad not in localidades:
        cursor.execute("INSERT INTO localidades (localidad) VALUES (%s)", (localidad,))
        localidades.append(localidad)

with open('Datos2017.csv', 'r') as file:
    reader = csv.reader(file)
    
    encabezados = next(reader)
    loci = encabezados.index('LOCALIDAD_TEX')

    for row in reader:
        
        loc = row[loci]
        agregarLocalidad(loc)
        #cursor.execute("INSERT INTO datos2017 (localidad) VALUES (%s)", (loc))
        # Procesar cada fila del archivo CSV
        # y ejecutar los comandos SQL correspondientes
        # para cargar los datos en las tablas de PostgreSQL
    pass  # Coloca aquí el código para procesar cada fila

# Dentro del bucle for anterior, puedes ejecutar los comandos SQL para insertar los datos en las tablas de PostgreSQL. Utiliza el cursor para ejecutar los comandos de inserción:

conn.commit()

conn.close()