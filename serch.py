from pathlib import Path
from dotenv import load_dotenv
import psycopg2
import os

load_dotenv()

db_nome = 'db_oscar'
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_SENHA')
db_host  = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')

connection = psycopg2.connect(
    dbname = db_nome,
    user = db_user,
    password = db_password,
    host = db_host,
    port = db_port
)

connection.autocommit = True
cursor = connection.cursor()

cursor.execute("""
                select nominees, count(*) as vitorias from tb_nominees
                where winner = true and category_id in (1,2,3,4)
                group by nominees order by vitorias DESC
               """)
resultado1 = cursor.fetchall()

cursor.execute("""
                select c.description as category, o.ceremony as ceremony, o.year as year from tb_nominees n
                join tb_category c on n.category_id = c.id
                join tb_oscar o on n.oscar_id = o.id
                join tb_movie m on n.movie_id = m.id
                where winner = TRUE and m.title = 'Interstellar'
               """)

resultado2 = cursor.fetchall()

cursor.execute("""
                select nominees, count(*) as indicacoes from tb_nominees
                where category_id = 12
                group by nominees
                having count(*) > 2
                order by count(*) DESC
               """)

resultado3 = cursor.fetchall()

cursor.close()
connection.close()

# Exportando resultados em .txt
caminho_arquivo_txt = Path(__file__).parent / 'relatorios/questao_1.txt'

with open(caminho_arquivo_txt, mode='w', encoding='utf-8') as file:
    file.write("Atores/atrizes que venceram mais vezes:\n")
    for nominees, vitorias in resultado1:
        file.write(f"{nominees}: {vitorias} vitórias\n")

caminho_arquivo_txt = Path(__file__).parent / 'relatorios/questao_2.txt'

with open(caminho_arquivo_txt, mode='w', encoding='utf-8') as file:
    file.write("Categorias que 'Interstellar' venceu:\n")
    for category, year, ceremony in resultado2:
        file.write(f"Categoria: {category} - Ano: {year} - Cerimonia: {ceremony}\n")

caminho_arquivo_txt = Path(__file__).parent / 'relatorios/questao_3.txt'

with open(caminho_arquivo_txt, mode='w', encoding='utf-8') as file:
    file.write("Diretores que foram indicados mais de 2 vezes:\n")
    for nomeinees, indicacoes in resultado3:
        file.write(f"{nomeinees}: {indicacoes} indicações\n")