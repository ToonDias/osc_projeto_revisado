from dotenv import load_dotenv
from pathlib import Path
import psycopg2
import os
import csv
import re

load_dotenv()

db_name = 'db_oscar'
db_user = os.getenv('DB_USER')
db_senha = os.getenv('DB_SENHA')
db_port = os.getenv('DB_PORT')
db_host  = os.getenv('DB_HOST')

CAMINHO_CSV = Path(__file__).parent / 'base/datasheet_oscars_complementar.csv'

with CAMINHO_CSV.open('r', encoding='utf-8') as arquivo:
    leitor = csv.DictReader(arquivo, delimiter='\t')
    linhas = list(leitor)

    list_oscar = []
    list_class = []
    list_category = []
    list_movie = []

    for linha in linhas:
        list_oscar.append((
                int(linha['Ceremony'].strip().replace('\n', '').replace('\r', '')), 
                int(linha['Year'].strip().replace('\n', '').replace('\r', '')),
                re.sub(r'^\d+\s+', '',linha['Location'].strip().replace('\n', '').replace('\r', ''))
            ))
        
        list_class.append(linha['Class'].strip().replace('\n', '').replace('\r', ''))

        list_category.append(linha['Category'].strip().replace('\n', '').replace('\r', ''))

        list_movie.append((
                linha['Movie'].strip().replace('\n', '').replace('\r', ''),
                linha['Movie Code'].strip().replace('\n', '').replace('\r', '')
            ))
    
lista_class_unique = [item for item in sorted(list(set(list_class))) if item]
lista_category_unique = [item for item in sorted(list(set(list_category))) if item]
lista_oscar_unique = [item for item in sorted(list(set(list_oscar))) if all(item)]    
lista_movie_unique = [item for item in sorted(list(set(list_movie))) if all(item)]

connection = psycopg2.connect(
    dbname = db_name,
    user = db_user,
    password = db_senha,
    port = db_port,
    host = db_host
)

connection.autocommit = True

cursor = connection.cursor()

for item in lista_class_unique:
    try:
        cursor.execute("insert into tb_class (description) values (%s)", (item,))
        print("Classe adicionada com sucesso!")
    except psycopg2.errors.UniqueViolation:
        print("Registro já existe!")

for item in lista_category_unique:
    try:
        cursor.execute("insert into tb_category (description) values (%s)", (item,))
        print("Categoria adicionada com sucesso!")
    except psycopg2.errors.UniqueViolation:
        print("Registro já existe!")

for item in lista_movie_unique:
    try:
        cursor.execute("insert into tb_movie (title, code) values (%s, %s)", item)
        print("Filme adicionada com sucesso!")
    except psycopg2.errors.UniqueViolation:
        print("Registro já existe!")

for item in lista_oscar_unique:
    try:
        cursor.execute("insert into tb_oscar (ceremony, year, location) values (%s, %s, %s)", item)
        print("Oscar adicionada com sucesso!")
    except psycopg2.errors.UniqueViolation:
        print("Registro já existe!")

cursor.close()
connection.close()