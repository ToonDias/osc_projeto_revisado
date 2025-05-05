from pathlib import Path
from dotenv import load_dotenv
import psycopg2
import csv
import os


load_dotenv()

db_name  = 'db_oscar'
db_senha = os.getenv('DB_SENHA')
db_user = os.getenv('DB_USER')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')

# print(db_name)
# print(db_senha)
# print(db_user)
# print(db_port)
# print(db_host)

connection = psycopg2.connect(
    dbname = db_name,
    user = db_user,
    password = db_senha,
    port = db_port,
    host = db_host
)

connection.autocommit = True
cursor = connection.cursor()

cursor.execute('select * from tb_class;')
all_class = cursor.fetchall()

cursor.execute('select * from tb_category;')
all_category = cursor.fetchall()

cursor.execute('select * from tb_oscar;')
all_oscar = cursor.fetchall()

cursor.execute('select * from tb_movie;')
all_movie = cursor.fetchall()

# print(all_class)
# print(all_category)
# print(all_oscar)
# print(all_movie)

CAMINHO_CSV =   Path(__file__).parent / 'base/datasheet_oscars_complementar.csv'

with CAMINHO_CSV.open('r', encoding='utf-8') as arquivo:
    leitor = csv.DictReader(arquivo, delimiter='\t')
    linhas = list(leitor)

    list_ceremony = []
    list_class = []
    list_category = []
    list_movie = []
    list_outros = []

    for linha in linhas[:2]:
        list_ceremony.append(int(linha['Ceremony'].strip().replace('\n', '').replace('\r', '')))
        list_class.append(linha['Class'].strip().replace('\n', '').replace('\r', ''))
        list_category.append(linha['Category'].strip().replace('\n', '').replace('\r', ''))
        list_movie.append(linha['Movie'].strip().replace('\n', '').replace('\r', ''))
        list_outros.append((
            linha['Name'].strip().replace('\n', '').replace('\r', '') if linha['Name'] else 'NF',
            linha['Nominees'].strip().replace('\n', '').replace('\r', '') if linha['Nominees'] else 'NF',
            True if linha['Winner'] else False,
            linha['Note'].strip().replace('\n', '').replace('\r', '') if linha['Note'] else 'NF'
        ))


for i in list_class:
    print(i)

print('-----------------')

for i in list_ceremony:
    print(i)

print('-----------------')

for i in list_category:
    print(i)

print('-----------------')

for i in list_movie:
    print(i)

print('-----------------')

for i in list_outros:
    print(i)