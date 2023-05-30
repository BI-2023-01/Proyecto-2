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

#Creación de tablas
cursor.execute("create table localidades (id serial primary key, localidad char(25));")
cursor.execute("create table fecha (id serial primary key, ano smallint);")
cursor.execute("create table humedad (id serial primary key, respuesta char(20));")
cursor.execute("create table ventilacion (id serial primary key, respuesta char(20));")
cursor.execute("create table fabricas (id serial primary key, respuesta char(2));")
cursor.execute("create table buses (id serial primary key, respuesta char(2));")
cursor.execute("create table contaminacionAire (id serial primary key, respuesta char(2));")
cursor.execute("create table combustibles (id serial primary key, respuesta char(50));")
#Crear la tabla de asma con foreign keys a los ids de las tablas anteriores
cursor.execute("create table asma (id serial primary key, asma char(2), localidad_id integer references localidades(id), fecha_id integer references fecha(id),\
               humedad_id integer references humedad(id), ventilacion_id integer references ventilacion(id), fabricas_id integer references fabricas(id),\
                buses_id integer references buses(id), contaminacionAire_id integer references contaminacionAire(id),\
                combustibles_id integer references combustibles(id));")

#Estructuras para guardar información
localidades = {}
humedades = []
ventilaciones = []
fabricas = []
buses = []
contaminacionAire = []
combustibles = []

#Insertar datos en tablas
cursor.execute("INSERT INTO fecha (ano) VALUES (2017)")
cursor.execute("INSERT INTO fecha (ano) VALUES (2021)")

def agregarLocalidad(localidad,codL):
    if localidad != 'NA':
        if localidad not in localidades:
            localidades[int(codL)]=localidad.upper()

def agregarHumedad():
    cursor.execute("INSERT INTO humedad (respuesta) VALUES ('Si')")
    humedades.append('Si')
    cursor.execute("INSERT INTO humedad (respuesta) VALUES ('No')")
    humedades.append('No')
    cursor.execute("INSERT INTO humedad (respuesta) VALUES ('No sabe/ No responde')")
    humedades.append('No sabe/ No responde')

def agregarVentilacion():
    cursor.execute("INSERT INTO ventilacion (respuesta) VALUES ('Si')")
    ventilaciones.append('Si')
    cursor.execute("INSERT INTO ventilacion (respuesta) VALUES ('No')")
    ventilaciones.append('No')
    cursor.execute("INSERT INTO ventilacion (respuesta) VALUES ('No sabe/ No responde')")
    ventilaciones.append('No sabe/ No responde')


def agregarFabricas():
    cursor.execute("INSERT INTO fabricas (respuesta) VALUES ('Si')")
    fabricas.append('Si')
    cursor.execute("INSERT INTO fabricas (respuesta) VALUES ('No')")
    fabricas.append('No')

def agregarBuses():
    cursor.execute("INSERT INTO buses (respuesta) VALUES ('Si')")
    buses.append('Si')
    cursor.execute("INSERT INTO buses (respuesta) VALUES ('No')")
    buses.append('No')

def agregarCAire():
    cursor.execute("INSERT INTO contaminacionAire (respuesta) VALUES ('Si')")
    contaminacionAire.append('Si')
    cursor.execute("INSERT INTO contaminacionAire (respuesta) VALUES ('No')")
    contaminacionAire.append('No')

def agregarCombustible():
    cursor.execute("INSERT INTO combustibles (respuesta) VALUES ('Electricidad')")
    combustibles.append('Electricidad')
    cursor.execute("INSERT INTO combustibles (respuesta) VALUES ('Gas natural conectado a red publica')")
    combustibles.append('Gas natural conectado a red publica')
    cursor.execute("INSERT INTO combustibles (respuesta) VALUES ('Gas propano en cilindro o pipeta')")
    combustibles.append('Gas propano en cilindro o pipeta')
    cursor.execute("INSERT INTO combustibles (respuesta) VALUES ('Petróleo, gasolina, kerosene, alcohol, cocinol')")
    combustibles.append('Petróleo, gasolina, kerosene, alcohol, cocinol')
    cursor.execute("INSERT INTO combustibles (respuesta) VALUES ('Carbon mineral')")
    combustibles.append('Carbon mineral')
    cursor.execute("INSERT INTO combustibles (respuesta) VALUES ('Carbon de leña')")
    combustibles.append('Carbon de leña')
    cursor.execute("INSERT INTO combustibles (respuesta) VALUES ('Leña, madera')")
    combustibles.append('Leña, madera')
    cursor.execute("INSERT INTO combustibles (respuesta) VALUES ('Material de desecho')")
    combustibles.append('Material de desecho')
    cursor.execute("INSERT INTO combustibles (respuesta) VALUES ('No sabe/ No responde')")
    combustibles.append('No sabe/ No responde')

def agregarAsma(asma, codL, hum, vent, fab, bus, con, comb):
    if asma == '1':
        cursor.execute(f"INSERT INTO asma (asma, localidad_id, fecha_id, humedad_id, ventilacion_id, fabricas_id, buses_id, contaminacionAire_id, combustibles_id) \
               VALUES ('Si', {codL}, 1, '{hum}', '{vent}', '{fab}', '{bus}', '{con}', {comb})")
    elif asma == '2':
        cursor.execute(f"INSERT INTO asma (asma, localidad_id, fecha_id, humedad_id, ventilacion_id, fabricas_id, buses_id, contaminacionAire_id, combustibles_id) \
               VALUES ('No', {codL}, 1, '{hum}', '{vent}', '{fab}', '{bus}', '{con}', {comb})")
    else:
        cursor.execute(f"INSERT INTO asma (asma, localidad_id, fecha_id, humedad_id, ventilacion_id, fabricas_id, buses_id, contaminacionAire_id, combustibles_id) \
               VALUES ('No sabe/ No responde', {codL}, 1, '{hum}', '{vent}', '{fab}', '{bus}', '{con}', {comb})")

def agregarAsma2(asma, codL, hum, vent, fab, bus, con, comb):
    if asma == '1':
        cursor.execute(f"INSERT INTO asma (asma, localidad_id, fecha_id, humedad_id, ventilacion_id, fabricas_id, buses_id, contaminacionAire_id, combustibles_id) \
               VALUES ('Si', {codL}, 2, '{hum}', '{vent}', '{fab}', '{bus}', '{con}', {comb})")
    elif asma == '2':
        cursor.execute(f"INSERT INTO asma (asma, localidad_id, fecha_id, humedad_id, ventilacion_id, fabricas_id, buses_id, contaminacionAire_id, combustibles_id) \
               VALUES ('No', {codL}, 2, '{hum}', '{vent}', '{fab}', '{bus}', '{con}', {comb})")
    else:
        cursor.execute(f"INSERT INTO asma (asma, localidad_id, fecha_id, humedad_id, ventilacion_id, fabricas_id, buses_id, contaminacionAire_id, combustibles_id) \
               VALUES ('No sabe/ No responde', {codL}, 2, '{hum}', '{vent}', '{fab}', '{bus}', '{con}', {comb})")

        

agregarHumedad()
agregarVentilacion()
agregarFabricas()
agregarBuses()
agregarCAire()
agregarCombustible()

#Agregar localidades
with open('Datos2017.csv', 'r') as file:
    reader = csv.reader(file)
    
    encabezados = next(reader)
    loci = encabezados.index('LOCALIDAD_TEX')
    codLi = encabezados.index('CODLOCALIDAD')

    for row in reader:
        
        loc = row[loci]
        codL = row[codLi]
        agregarLocalidad(loc,codL)


for i in range(1, len(localidades.keys())+1):
    cursor.execute("INSERT INTO localidades (localidad) VALUES ('"+localidades[i]+"')")


# Preprocesamiento de datos
# Valores vacios para la columna localidad

import pandas as pd

# Lee el archivo Excel
df = pd.read_csv('Datos2017.csv')

# Reemplaza 'NA' con la moda en la columna deseada
localidad = 'CODLOCALIDAD'
moda_localidad = str(int(float(pd.Series.mode(df[localidad])[0])))

combustible = 'NHCCP26'
moda_combustible = str(int(float(pd.Series.mode(df[combustible])[0])))

#Agregar registros
with open('Datos2017.csv', 'r') as file:
    reader = csv.reader(file)
    
    encabezados = next(reader)
    codLi = encabezados.index('CODLOCALIDAD')
    humi = encabezados.index('NVCBP8A')
    venti = encabezados.index('NVCBP8G')
    fabri = encabezados.index('NVCBP14A')
    busi = encabezados.index('NVCBP14D')
    coni = encabezados.index('NVCBP15D')
    combi = encabezados.index('NHCCP26')
    asmai = encabezados.index('NPCFP14I')

    for row in reader:
        codL=row[codLi]
        if codL == '':
            codL = moda_localidad
        elif codL == 'NA':
            codL = moda_localidad
        hum=row[humi]
        if hum == '9':
            hum = '3'
        vent=row[venti]
        if vent == '9':
            vent = '3'
        fab=row[fabri]
        bus=row[busi]
        con=row[coni]
        comb=row[combi]
        asma=row[asmai]
        if comb == '':
            comb = moda_combustible
        elif comb == 'NA':
            comb = moda_combustible
        agregarAsma(asma,codL,hum,vent,fab,bus,con,comb)

# Lee el archivo Excel
df = pd.read_csv('Datos2021_no.csv', encoding='ISO-8859-1')

# Reemplaza 'NA' con la moda en la columna deseada
localidad = 'COD_LOCALIDAD'
moda_localidad = str(int(float(pd.Series.mode(df[localidad])[0])))

combustible = 'NHCCP26'
moda_combustible = str(int(float(pd.Series.mode(df[combustible])[0])))

with open('Datos2021_no.csv', 'r') as file:
    reader = csv.reader(file)
    
    encabezados = next(reader)
    codLi = encabezados.index('COD_LOCALIDAD')
    humi = encabezados.index('NVCBP8A')
    venti = encabezados.index('NVCBP8G')
    fabri = encabezados.index('NVCBP14A')
    busi = encabezados.index('NVCBP14D')
    coni = encabezados.index('NVCBP15D')
    combi = encabezados.index('NHCCP26')
    asmai = encabezados.index('NPCFP14F')

    for row in reader:
        codL=row[codLi]
        if codL == '':
            codL = moda_localidad
        elif codL == 'NA':
            codL = moda_localidad
        hum=row[humi]
        if hum == '9':
            hum = '3'
        vent=row[venti]
        if vent == '9':
            vent = '3'
        fab=row[fabri]
        bus=row[busi]
        con=row[coni]
        comb=row[combi]
        asma=row[asmai]
        if comb == '':
            comb = moda_combustible
        elif comb == 'NA':
            comb = moda_combustible
        agregarAsma2(asma,codL,hum,vent,fab,bus,con,comb)

# Lee el archivo Excel
df = pd.read_csv('Datos2021_si.csv', encoding='ISO-8859-1')

# Reemplaza 'NA' con la moda en la columna deseada
localidad = 'COD_LOCALIDAD'
moda_localidad = str(int(float(pd.Series.mode(df[localidad])[0])))

combustible = 'NHCCP26'
moda_combustible = str(int(float(pd.Series.mode(df[combustible])[0])))

with open('Datos2021_si.csv', 'r') as file:
    reader = csv.reader(file)
    
    encabezados = next(reader)
    codLi = encabezados.index('COD_LOCALIDAD')
    humi = encabezados.index('NVCBP8A')
    venti = encabezados.index('NVCBP8G')
    fabri = encabezados.index('NVCBP14A')
    busi = encabezados.index('NVCBP14D')
    coni = encabezados.index('NVCBP15D')
    combi = encabezados.index('NHCCP26')
    asmai = encabezados.index('NPCFP14F')

    for row in reader:
        codL=row[codLi]
        if codL == '':
            codL = moda_localidad
        elif codL == 'NA':
            codL = moda_localidad
        hum=row[humi]
        if hum == '9':
            hum = '3'
        vent=row[venti]
        if vent == '9':
            vent = '3'
        fab=row[fabri]
        bus=row[busi]
        con=row[coni]
        comb=row[combi]
        asma=row[asmai]
        if comb == '':
            comb = moda_combustible
        elif comb == 'NA':
            comb = moda_combustible
        agregarAsma2(asma,codL,hum,vent,fab,bus,con,comb)

conn.commit()

conn.close()