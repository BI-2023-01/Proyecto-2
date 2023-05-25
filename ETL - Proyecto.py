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

cursor.execute("create table datos2017 (ZIP char(5));")

with open('Datos2017.csv', 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Omitir la primera fila si contiene encabezados
    for row in reader:
        # Procesar cada fila del archivo CSV
        # y ejecutar los comandos SQL correspondientes
        # para cargar los datos en las tablas de PostgreSQL
        pass  # Coloca aquí el código para procesar cada fila

# Dentro del bucle for anterior, puedes ejecutar los comandos SQL para insertar los datos en las tablas de PostgreSQL. Utiliza el cursor para ejecutar los comandos de inserción:



conn.commit()

conn.close()