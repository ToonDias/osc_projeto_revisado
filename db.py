from dotenv import load_dotenv
import psycopg2
import os

load_dotenv()


db_nome = os.getenv("DB_NOME")
db_user = os.getenv("DB_USER")
db_senha = os.getenv("DB_SENHA")
db_port = os.getenv("DB_PORT")
db_host = os.getenv("DB_HOST")

# print(db_nome)
# print(db_user)
# print(db_senha)
# print(db_port)
# print(db_host)

connection = psycopg2.connect(
    dbname = db_nome,
    user = db_user,
    password = db_senha,
    host = db_host,
    port = db_port
    )

connection.autocommit = True

cursor = connection.cursor()

try:
    cursor.execute("create database db_oscar")
    print("Banco de dados criado!!")
except psycopg2.errors.DuplicateDatabase:
    print("Banco de dados já existe")

cursor.close()
connection.close()


connection = psycopg2.connect(
    dbname = 'db_oscar',
    user = db_user,
    password = db_senha,
    host = db_host,
    port = db_port
)

connection.autocommit = True

cursor  = connection.cursor()

try:
    cursor.execute("create table tb_class (id bigserial not null, description varchar(255) not null, primary key (id), unique (description))")
    print("Tabela tb_class criada com sucesso!")
except psycopg2.errors.DuplicateTable:
    print("Tabela tb_class já existe!")

try:
    cursor.execute("create table tb_movie (id bigserial not null, title varchar(500) not null, primary key (id), unique (title))")
    print("tabela tb_movie criada com sucesso!")
except psycopg2.errors.DuplicateTable:
    print("Tabela tb_movie já existe!")

try:
    cursor.execute("create table tb_category (id bigserial not null, description varchar(500) not null, primary key (id), unique (description))")
    print("Tabela tb_category criada com sucesso!")
except psycopg2.errors.DuplicateTable:
    print("Tabeça tb_caregory já existe!")

try:
    cursor.execute("create table tb_oscar (id bigserial not null, ceremony int not null, year int not null)")
    print("Tabela tb_oscar criada com sucesso!")
except psycopg2.errors.DuplicateTable:
    print("Tabela tb_oscar já existe!")

try:
    cursor.execute("create table tb_nominees (id bigserial not null, ceremony_id bigint not null, class_id bigint not null," \
    "category_id bigint not null, movie_id bigint not null, detail text, note text, primary key (id))")
    print("Tabela tb_nominees criada com sucesso!")
except psycopg2.errors.DuplicateTable:
    print("Tabela tb_nominees já existe!")

try:
    cursor.execute("alter table tb_movie add column code varchar(255) not null ")
    print("Coluna code adicionada com sucesso!")
except psycopg2.errors.DuplicateColumn:
    print("Coluna code já existe!")

try:
    cursor.execute("alter table tb_oscar add column location text")
    print("Coluna location adicionada com sucesso!")
except psycopg2.errors.DuplicateColumn:
    print("Coluna location já existe!")

cursor.close()
connection.close()