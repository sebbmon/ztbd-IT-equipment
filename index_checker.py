import psycopg2
import mysql.connector
from pymongo import MongoClient
from cassandra.cluster import Cluster

# ==========================================
# POŁĄCZENIA
# ==========================================
def connect_all():
    print("Nawiązywanie połączeń z bazami...")
    pg = psycopg2.connect(host="localhost", port=5432, user="admin", password="password", dbname="it_equipment")
    my = mysql.connector.connect(host="localhost", port=3306, user="admin", password="password", database="it_equipment")
    mongo = MongoClient("mongodb://admin:password@localhost:27017/?authSource=admin")["it_equipment"]
    cass = Cluster(['localhost'], port=9042).connect('it_equipment')
    return pg, my, mongo, cass

# ==========================================
# WERYFIKACJA INDEKSÓW
# ==========================================
def check_postgres(pg):
    print("\n" + "="*50)
    print(" POSTGRESQL - AKTUALNE INDEKSY")
    print("="*50)
    cur = pg.cursor()
    cur.execute("""
        SELECT tablename, indexname, indexdef 
        FROM pg_indexes 
        WHERE schemaname = 'public' 
        ORDER BY tablename, indexname;
    """)
    rows = cur.fetchall()
    if not rows:
        print("Brak indeksów w schemacie publicznym.")
    for row in rows:
        print(f"Tabela: {row[0]:<15} | Indeks: {row[1]:<25} | Definicja: {row[2]}")
    cur.close()

def check_mysql(my):
    print("\n" + "="*50)
    print(" MYSQL - AKTUALNE INDEKSY")
    print("="*50)
    cur = my.cursor()
    cur.execute("""
        SELECT TABLE_NAME, INDEX_NAME, COLUMN_NAME, INDEX_TYPE 
        FROM information_schema.STATISTICS 
        WHERE TABLE_SCHEMA = 'it_equipment' 
        ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX;
    """)
    rows = cur.fetchall()
    if not rows:
        print("Brak indeksów w bazie it_equipment.")
    for row in rows:
        print(f"Tabela: {row[0]:<15} | Indeks: {row[1]:<20} | Kolumna: {row[2]:<15} | Typ: {row[3]}")
    cur.close()

def check_mongo(mongo):
    print("\n" + "="*50)
    print(" MONGODB - AKTUALNE INDEKSY")
    print("="*50)
    collections = mongo.list_collection_names()
    has_indexes = False
    
    for coll in collections:
        indexes = mongo[coll].index_information()
        for index_name, index_info in indexes.items():
            has_indexes = True
            keys = ", ".join([f"{k[0]} ({k[1]})" for k in index_info.get('key', [])])
            print(f"Kolekcja: {coll:<15} | Indeks: {index_name:<20} | Pola: {keys}")
            
    if not has_indexes:
        print("Brak kolekcji lub indeksów.")

def check_cassandra(cass):
    print("\n" + "="*50)
    print(" CASSANDRA - AKTUALNE INDEKSY (Secondary Indexes)")
    print("="*50)
    rows = cass.execute("""
        SELECT table_name, index_name, kind, options 
        FROM system_schema.indexes 
        WHERE keyspace_name = 'it_equipment';
    """)
    rows_list = list(rows)
    if not rows_list:
        print("Brak dodatkowych indeksów (Secondary Indexes). Widoczne są tylko Primary Keys.")
    for row in rows_list:
        target = row.options.get('target', 'N/A')
        print(f"Tabela: {row.table_name:<15} | Indeks: {row.index_name:<25} | Kolumna: {target} | Typ: {row.kind}")

# ==========================================
# MAIN
# ==========================================
if __name__ == "__main__":
    try:
        pg, my, mongo, cass = connect_all()
        
        check_postgres(pg)
        check_mysql(my)
        check_mongo(mongo)
        check_cassandra(cass)
        
    except Exception as e:
        print(f"\nWystąpił błąd podczas sprawdzania indeksów: {e}")
    finally:
        if 'pg' in locals(): pg.close()
        if 'my' in locals(): my.close()
        if 'cass' in locals(): cass.cluster.shutdown()
        print("\n[Zakończono weryfikację]")