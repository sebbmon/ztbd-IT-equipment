import psycopg2
import mysql.connector
from pymongo import MongoClient
from cassandra.cluster import Cluster

TABELE = [
    "budynek", "dzial", "producent", "karta", "przetarg", 
    "lokalizacja", "model", "pracownik", "urzadzenie", "historiaoperacji"
]

def czysc_bazy():
    print("="*50)
    print("[TRWA] CZYSZCZENIE BAZ DANYCH (TRUNCATE)")
    print("="*50)

    # 1. postgres
    try:
        print("[TRWA] Czyszczenie PostgreSQL...", end="", flush=True)
        pg = psycopg2.connect(host="localhost", port=5432, user="admin", password="password", dbname="it_equipment")
        cur_pg = pg.cursor()
        tabel_str = ", ".join(TABELE)
        cur_pg.execute(f"TRUNCATE TABLE {tabel_str} CASCADE;")
        pg.commit()
        pg.close()
        print(" [OK]")
    except Exception as e:
        print(f" [BŁĄD] {e}")

    # 2. mysql
    try:
        print("[TRWA] Czyszczenie MySQL...", end="", flush=True)
        my = mysql.connector.connect(host="localhost", port=3306, user="admin", password="password", database="it_equipment")
        cur_my = my.cursor()
        cur_my.execute("SET FOREIGN_KEY_CHECKS = 0;")
        for tabela in TABELE:
            cur_my.execute(f"TRUNCATE TABLE {tabela};")
        cur_my.execute("SET FOREIGN_KEY_CHECKS = 1;")
        my.commit()
        my.close()
        print(" [OK]")
    except Exception as e:
        print(f" [BŁĄD] {e}")

    # 3. mongo
    try:
        print("[TRWA] Czyszczenie MongoDB...", end="", flush=True)
        mongo = MongoClient("mongodb://admin:password@localhost:27017/?authSource=admin")["it_equipment"]
        for tabela in TABELE:
            mongo[tabela].delete_many({})
        print(" [OK]")
    except Exception as e:
        print(f" [BŁĄD] {e}")

    # 4. cassandra
    try:
        print("[TRWA] Czyszczenie Cassandra...", end="", flush=True)
        cluster = Cluster(['localhost'], port=9042)
        cass = cluster.connect('it_equipment')
        cass.default_timeout = 60.0
        for tabela in TABELE:
            cass.execute(f"TRUNCATE {tabela};")
        cluster.shutdown()
        print(" [OK]")
    except Exception as e:
        print(f" [BŁĄD] {e}")

    print("="*50)
    print("PROCES CZYSZCZENIA ZAKOŃCZONY.")
    print("="*50)

if __name__ == "__main__":
    czysc_bazy()