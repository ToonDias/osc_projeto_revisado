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

CAMINHO_CSV = Path(__file__).parent / 'base/datasheet_oscars_complementar.csv'

with CAMINHO_CSV.open('r', encoding='utf-8') as arquivo:
    leitor = csv.DictReader(arquivo, delimiter='\t')
    linhas = list(leitor)

    linhas_invalidas_obg = []
    linhas_invalidas_nsc = []
    linhas_validas = []
    lista_dados = []

    for linha in linhas:
        if not linha['Ceremony'] and not linha['Year'] or not linha['Class'] or not linha['Category']  or not linha['Movie']:
            linhas_invalidas_obg.append(linha)
            continue
        elif not linha['Nominees']:
            linhas_invalidas_nsc.append(linha)
            continue
        else:
            linhas_validas.append(linha)

            Ceremony = int(str(linha['Ceremony']).strip().replace('\n', '').replace('\r', ''))
            Year = int(str(linha['Year']).strip().replace('\n', '').replace('\r', ''))
            Class  = str(linha['Class']).strip().replace('\n', '').replace('\r', '')
            Category = str(linha['Category']).strip().replace('\n', '').replace('\r', '')
            Movie  = str(linha['Movie']).strip().replace('\n', '').replace('\r', '')
            Name = str(linha['Name']).strip().replace('\n', '').replace('\r', '') if linha['Name'] else 'NF'
            Nominees = str(linha['Nominees']).strip().replace('\n', '').replace('\r', '') if linha['Nominees'] else 'NF'
            Winner =   True if linha['Winner'] else False
            Detail = str(linha['Detail']).strip().replace('\n', '').replace('\r', '') if linha['Detail'] else 'NF'
            Note = str(linha['Note']).strip().replace('\n', '').replace('\r', '') if linha['Note'] else 'NF'

            lista_dados.append((
                next((item[0] for item in all_oscar if Ceremony == item[1] or Year == item[2]), None),
                next((item[0] for item in all_class if Class == item[1]), None),
                next((item[0] for item in all_category if Category == item[1]), None),
                next((item[0] for item in all_movie if Movie == item[1]), None),
                Name,
                Nominees,
                Winner,
                Detail,
                Note
            ))       

cursor.executemany('insert into tb_nominees (oscar_id, class_id, category_id, movie_id, name, nominees, winner, detail, note) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)', lista_dados)

print('Registros adicionado a tabela tb_nominnes com sucesso!')
cursor.close()
connection.close()

# Emissão dos relatorios em .csv
CAMINHO_RELATORIO = Path(__file__).parent / 'relatorios/linhas_validas.csv'

with CAMINHO_RELATORIO.open('w', newline='', encoding='utf-8') as file:
    fieldname = linhas_validas[0].keys()
    write = csv.DictWriter(file, fieldnames=fieldname)
    write.writeheader()
    write.writerows(linhas_validas)
    print("Dados validos salvos em .csv")

CAMINHO_RELATORIO = Path(__file__).parent / 'relatorios/linhas_invalidas_obg.csv'

with CAMINHO_RELATORIO.open('w', newline='', encoding='utf-8') as file:
    fieldname = linhas_invalidas_obg[0].keys()
    write = csv.DictWriter(file, fieldnames=fieldname)
    write.writeheader()
    write.writerows(linhas_invalidas_obg)
    print("Dados invalidos obrigatorios salvos em .csv")

CAMINHO_RELATORIO = Path(__file__).parent / 'relatorios/linhas_invalidas_nsc.csv'

with CAMINHO_RELATORIO.open('w', newline='', encoding='utf-8') as file:
    fieldname = linhas_invalidas_nsc[0].keys()
    write = csv.DictWriter(file, fieldnames=fieldname)
    write.writeheader()
    write.writerows(linhas_invalidas_nsc)
    print("Dados invalidos necessarios salvos em .csv")