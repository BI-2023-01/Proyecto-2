import psycopg2
import csv

conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="Cats",
    user="postgres",
    password="postgres"
)

cursor = conn.cursor()

with open('Datos2017.csv', 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Omitir la primera fila si contiene encabezados
    for row in reader:
        # Procesar cada fila del archivo CSV
        # y ejecutar los comandos SQL correspondientes
        # para cargar los datos en las tablas de PostgreSQL
        pass  # Coloca aquí el código para procesar cada fila

# Dentro del bucle for anterior, puedes ejecutar los comandos SQL para insertar los datos en las tablas de PostgreSQL. Utiliza el cursor para ejecutar los comandos de inserción:
cursor.execute("INSERT INTO nombre_de_tabla (columna1, columna2, ...) VALUES (%s, %s, ...)", (valor1, valor2, ...))


conn.commit()

conn.close()