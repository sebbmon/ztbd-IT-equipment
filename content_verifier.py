import psycopg2
import mysql.connector
from pymongo import MongoClient
from cassandra.cluster import Cluster

TABELE = [
    "budynek", "dzial", "producent", "karta", "przetarg", 
    "lokalizacja", "model", "pracownik", "urzadzenie", "historiaoperacji"
]

def weryfikuj_stan_baz():
    print("="*50)
    print(" WERYFIKACJA ZAWARTOŚCI TABEL W BAZACH DANYCH")
    print("="*50)

    # 1. postgres
    print("\n[ PostgreSQL ]")
    try:
        pg = psycopg2.connect(host="localhost", port=5432, user="admin", password="password", dbname="it_equipment")
        cur_pg = pg.cursor()
        for tabela in TABELE:
            cur_pg.execute(f"SELECT COUNT(*) FROM {tabela};")
            liczba = cur_pg.fetchone()[0]
            print(f"  - {tabela.ljust(20)} : {liczba} rekordów")
        pg.close()
    except Exception as e:
        print(f"  [BŁĄD POŁĄCZENIA] {e}")

    # 2. mysql
    print("\n[ MySQL ]")
    try:
        my = mysql.connector.connect(host="localhost", port=3306, user="admin", password="password", database="it_equipment")
        cur_my = my.cursor()
        for tabela in TABELE:
            cur_my.execute(f"SELECT COUNT(*) FROM {tabela};")
            liczba = cur_my.fetchone()[0]
            print(f"  - {tabela.ljust(20)} : {liczba} rekordów")
        my.close()
    except Exception as e:
        print(f"  [BŁĄD POŁĄCZENIA] {e}")

    # 3. mongo
    print("\n[ MongoDB ]")
    try:
        mongo = MongoClient("mongodb://admin:password@localhost:27017/?authSource=admin")["it_equipment"]
        for tabela in TABELE:
            liczba = mongo[tabela].count_documents({})
            print(f"  - {tabela.ljust(20)} : {liczba} rekordów")
    except Exception as e:
        print(f"  [BŁĄD POŁĄCZENIA] {e}")

    # 4. cassandra
    print("\n[ Cassandra ]")
    try:
        cluster = Cluster(['localhost'], port=9042)
        cass = cluster.connect('it_equipment')
        cass.default_timeout = 60.0
        for tabela in TABELE:
            liczba = cass.execute(f"SELECT COUNT(*) FROM {tabela};", timeout=None).one()[0]
            print(f"  - {tabela.ljust(20)} : {liczba} rekordów")
        cluster.shutdown()
    except Exception as e:
        print(f"  [BŁĄD POŁĄCZENIA] {e}")

    print("\n" + "="*50)
    print(" WERYFIKACJA ZAKOŃCZONA.")
    print("="*50)

if __name__ == "__main__":
    weryfikuj_stan_baz()