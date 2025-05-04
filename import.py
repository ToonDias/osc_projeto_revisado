from dotenv import load_dotenv
from pathlib import Path
import psycopg2
import os
import csv
import re

load_dotenv()

db_name = os.getenv('DB_nome')
db_user = os.getenv('DB_USER')
db_senha = os.getenv('DB_SENHA')
db_port = os.getenv('DB_PORT')
db_host  = os.getenv('DB_HOST')

# print(db_name)
# print(db_senha)
# print(db_user)
# print(db_port)
# print(db_host)

CAMINHO_CSV = Path(__file__).parent / 'base/datasheet_oscars_complementar.csv'

# print(CAMINHO_CSV)

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
                int(linha['Year'][:4].strip().replace('\n', '').replace('\r', '')), 
                re.sub(r'^\d+\s+', '',linha['Location'].strip().replace('\n', '').replace('\r', ''))
            ))
        
        list_class.append(linha['Class'].strip().replace('\n', '').replace('\r', ''))

        list_category.append(linha['Category'].strip().replace('\n', '').replace('\r', ''))

        list_movie.append((
                linha['Movie'].strip().replace('\n', '').replace('\r', ''),
                linha['Movie Code'].strip().replace('\n', '').replace('\r', '')
            ))
    
lista_class_unique = [item for item in sorted(list(set(list_class))) if item]
list_oscar_unique = [item for item in sorted(list(set(list_oscar))) if all(item)]    
lista_category_unique = [item for item in sorted(list(set(list_category))) if item]
lista_movie_unique = [item for item in sorted(list(set(list_movie))) if all(item)]

connection = psycopg2.connect(
    dbname = 'db_oscar',
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

for item in lista_class_unique:
    try:
        cursor.execute("insert into tb_class (description) values (%s)", (item,))
        print("Classe adicionada com sucesso!")
    except psycopg2.errors.UniqueViolation:
        print("Registro já existe!")

