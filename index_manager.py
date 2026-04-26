import sys
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
    pg.autocommit = True
    my = mysql.connector.connect(host="localhost", port=3306, user="admin", password="password", database="it_equipment")
    my.autocommit = True
    mongo = MongoClient("mongodb://admin:password@localhost:27017/?authSource=admin")["it_equipment"]
    cass = Cluster(['localhost'], port=9042).connect('it_equipment')
    return pg, my, mongo, cass

# ==========================================
# USUWANIE INDEKSÓW (POWRÓT DO FAZY 1)
# ==========================================
def drop_indexes(pg, my, mongo, cass):
    print("\n[FAZA 1] Usuwanie indeksów niestandardowych (Czyszczenie)...")
    
    # 1. POSTGRESQL
    print(" -> Czyszczenie PostgreSQL...")
    with pg.cursor() as cur:
        indexes_to_drop = [
            "idx_urzadzenie_nazwa", "idx_urzadzenie_stan", "idx_urzadzenie_modelid",
            "idx_historia_data", "idx_historia_pracownik"
        ]
        for idx in indexes_to_drop:
            cur.execute(f"DROP INDEX IF EXISTS {idx};")
    
    # 2. MYSQL
    print(" -> Czyszczenie MySQL...")
    with my.cursor() as cur:
        indexes_to_drop_my = [
            ("urzadzenie", "idx_urzadzenie_nazwa"),
            ("urzadzenie", "idx_urzadzenie_stan"),
            ("historiaoperacji", "idx_historia_data")
        ]
        for table, idx in indexes_to_drop_my:
            try:
                cur.execute(f"ALTER TABLE {table} DROP INDEX {idx};")
            except mysql.connector.Error:
                pass # Ignoruj, jeśli indeksu już nie ma
                
    # 3. MONGODB
    print(" -> Czyszczenie MongoDB...")
    cols_to_clear = ["urzadzenie", "historiaoperacji", "budynek", "pracownik"]
    for coll in cols_to_clear:
        # Metoda drop_indexes() usuwa wszystkie indeksy oprócz wbudowanego '_id_'
        try:
            mongo[coll].drop_indexes()
        except Exception:
            pass
        
    # 4. CASSANDRA
    print(" -> Czyszczenie Cassandry...")
    indexes_cass = ["idx_urzadzenie_nazwa", "idx_urzadzenie_stan", "idx_historia_data"]
    for idx in indexes_cass:
        cass.execute(f"DROP INDEX IF EXISTS {idx};")
        
    print("\n[OK] Bazy są sterylnie czyste. Środowisko gotowe do testów 'BEZ INDEKSÓW' (Faza 1).")

# ==========================================
# TWORZENIE INDEKSÓW (PRZEJŚCIE DO FAZY 2)
# ==========================================
def create_indexes(pg, my, mongo, cass):
    print("\n[FAZA 2] Budowanie indeksów niestandardowych (To może potrwać kilka minut przy milionach rekordów!)...")
    
    # 1. POSTGRESQL
    print(" -> Budowanie PostgreSQL...")
    with pg.cursor() as cur:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_urzadzenie_nazwa ON urzadzenie(nazwa);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_urzadzenie_stan ON urzadzenie(stan);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_urzadzenie_modelid ON urzadzenie(modelid);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_historia_data ON historiaoperacji(data_zdarzenia);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_historia_pracownik ON historiaoperacji(pracownikid);")
        
    # 2. MYSQL
    print(" -> Budowanie MySQL...")
    with my.cursor() as cur:
        indexes_to_create = [
            ("CREATE INDEX idx_urzadzenie_nazwa ON urzadzenie(nazwa);", "urzadzenie", "idx_urzadzenie_nazwa"),
            ("CREATE INDEX idx_urzadzenie_stan ON urzadzenie(stan);", "urzadzenie", "idx_urzadzenie_stan"),
            ("CREATE INDEX idx_historia_data ON historiaoperacji(data_zdarzenia);", "historiaoperacji", "idx_historia_data")
        ]
        for query, table, idx in indexes_to_create:
            try:
                cur.execute(query)
            except mysql.connector.Error:
                pass # Ignoruj, jeśli indeks już istnieje

    # 3. MONGODB
    print(" -> Budowanie MongoDB (w tym naprawa architektury ID)...")
    # Naprawa braku indeksów na kluczach głównych 'id' w naszej strukturze NoSQL
    mongo["urzadzenie"].create_index([("id", 1)], name="idx_urzadzenie_id")
    mongo["historiaoperacji"].create_index([("id", 1)], name="idx_historia_id")
    mongo["budynek"].create_index([("id", 1)], name="idx_budynek_id")
    mongo["pracownik"].create_index([("id", 1)], name="idx_pracownik_id")
    
    # Indeksy biznesowe
    mongo["urzadzenie"].create_index([("nazwa", 1)], name="idx_urzadzenie_nazwa")
    mongo["urzadzenie"].create_index([("stan", 1)], name="idx_urzadzenie_stan")
    mongo["historiaoperacji"].create_index([("data_zdarzenia", 1)], name="idx_historia_data")
    mongo["historiaoperacji"].create_index([("pracownikid", 1)], name="idx_historia_pracownik")

    # 4. CASSANDRA
    print(" -> Budowanie Cassandry (Secondary Indexes)...")
    queries_cass = [
        "CREATE INDEX IF NOT EXISTS idx_urzadzenie_nazwa ON urzadzenie(nazwa);",
        "CREATE INDEX IF NOT EXISTS idx_urzadzenie_stan ON urzadzenie(stan);",
        "CREATE INDEX IF NOT EXISTS idx_historia_data ON historiaoperacji(data_zdarzenia);"
    ]
    for q in queries_cass:
        cass.execute(q)
        
    print("\n[OK] Drzewa B-Tree ułożone. Środowisko gotowe do testów 'Z INDEKSAMI' (Faza 2).")

# ==========================================
# MAIN INTERFACE
# ==========================================
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("BŁĄD: Podaj komendę do wykonania!")
        print("Użycie:")
        print("  python index_manager.py drop   -> Przywraca bazę do stanu surowego (Faza 1)")
        print("  python index_manager.py create -> Tworzy wszystkie indeksy (Faza 2)")
        sys.exit(1)
        
    akcja = sys.argv[1].lower()
    
    try:
        pg, my, mongo, cass = connect_all()
        
        if akcja == "drop":
            drop_indexes(pg, my, mongo, cass)
        elif akcja == "create":
            create_indexes(pg, my, mongo, cass)
        else:
            print("Nieznana komenda. Użyj 'drop' lub 'create'.")
            
    except Exception as e:
        print(f"\n[FATAL ERROR] Wyjątek podczas zarządzania indeksami: {e}")
    finally:
        if 'pg' in locals() and pg: pg.close()
        if 'my' in locals() and my: my.close()
        if 'cass' in locals() and cass: cass.cluster.shutdown()