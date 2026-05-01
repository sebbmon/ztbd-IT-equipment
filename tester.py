import time
import json
import random
import psycopg2
import psycopg2.extras
import mysql.connector
from pymongo import MongoClient
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
import numpy as np
from datetime import datetime
import sys

# ==========================================
# KONFIGURACJA TESTÓW
# ==========================================
REPEATS = 5

try:
    CURRENT_SIZE = int(sys.argv[1])
except IndexError:
    print("Użycie: python tester.py [ROZMIAR_BAZY] (np. python tester.py 500000)")
    sys.exit(1)

# ==========================================
# POŁĄCZENIA
# ==========================================
def connect_all():
    print("Nawiązywanie połączeń...")
    pg = psycopg2.connect(host="localhost", port=5432, user="admin", password="password", dbname="it_equipment")
    pg.autocommit = True
    my = mysql.connector.connect(host="localhost", port=3306, user="admin", password="password", database="it_equipment")
    my.autocommit = True
    mongo = MongoClient("mongodb://admin:password@localhost:27017/?authSource=admin")["it_equipment"]
    cass = Cluster(['localhost'], port=9042).connect('it_equipment')
    return pg, my, mongo, cass

class DatabaseTester:
    def __init__(self, size):
        self.size = size
        self.pg, self.my, self.mo, self.ca = connect_all()
        self.results = []
        
        self.cass_insert_u = self.ca.prepare(
            "INSERT INTO urzadzenie (id, nazwa, przetargid, numerseryjny, stan, modelid) VALUES (?, ?, ?, ?, ?, ?)"
        )
        self.cass_delete_budynek = self.ca.prepare(
            "DELETE FROM budynek WHERE id = ?"
        )

    def measure(self, pg_f, my_f, mo_f, ca_f, param_gen=None):
        def run_db(func, db_name):
            if not func: return -1.0
            times = []
            for _ in range(REPEATS):
                p = param_gen() if param_gen else None
                start = time.perf_counter()
                try:
                    if p:
                        func(p)
                    else:
                        func()
                    times.append((time.perf_counter() - start) * 1000)
                except Exception as e:
                    print(f"\n[BŁĄD {db_name.upper()}]: {e}")
                    return -1.0 
            return float(np.mean(times))

        return run_db(pg_f, "pg"), run_db(my_f, "my"), run_db(mo_f, "mo"), run_db(ca_f, "ca")

    def log_result(self, category, name, t_pg, t_my, t_mo, t_ca):
        self.results.append({
            "category": category, "scenario": name,
            "postgres": t_pg, "mysql": t_my, "mongo": t_mo, "cassandra": t_ca
        })
        print(f"| {category} | {name:<28} | {t_pg:8.2f} ms | {t_my:8.2f} ms | {t_mo:8.2f} ms | {t_ca:8.2f} ms |")

    def run(self):
        pg_c = self.pg.cursor()
        my_c = self.my.cursor(buffered=True)
        s = self.size

        print(f"\n{'='*80}\n ROZPOCZYNAM TESTY (WOLUMEN: {s} REKORDÓW)\n{'='*80}")
        print(f"| {'TYP':<6} | {'SCENARIUSZ':<28} | {'POSTGRES':<11} | {'MYSQL':<11} | {'MONGO':<11} | {'CASSANDRA':<11} |")
        print("-" * 80)

        # =====================================================================
        # 1. CREATE (Indeksy zazwyczaj zwalniają te operacje - narzut zapisu!)
        # =====================================================================
        def c_params(): return (random.randint(50_000_000, 60_000_000), f"Urzadzenie-X", 1, "SN-X", "Nowy", 1)
        
        self.log_result("CREATE", "C1_Single_Insert", *self.measure(
            lambda p: pg_c.execute("INSERT INTO urzadzenie VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", p),
            lambda p: my_c.execute("INSERT IGNORE INTO urzadzenie VALUES (%s, %s, %s, %s, %s, %s)", p),
            lambda p: self.mo["urzadzenie"].insert_one({"id": p[0], "nazwa": p[1], "przetargid": p[2], "numerseryjny": p[3], "stan": p[4], "modelid": p[5]}),
            lambda p: self.ca.execute("INSERT INTO urzadzenie (id, nazwa, przetargid, numerseryjny, stan, modelid) VALUES (%s, %s, %s, %s, %s, %s)", p),
            c_params
        ))

        def c_batch_params(): 
            return [(random.randint(60_000_001, 70_000_000), "Batch", 1, "SN", "Nowy", 1) for _ in range(500)]
        
        self.log_result("CREATE", "C2_Batch_Insert_500", *self.measure(
            lambda p: psycopg2.extras.execute_batch(pg_c, "INSERT INTO urzadzenie VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING", p),
            lambda p: my_c.executemany("INSERT IGNORE INTO urzadzenie VALUES (%s, %s, %s, %s, %s, %s)", p),
            lambda p: self.mo["urzadzenie"].insert_many([{"id": r[0], "nazwa": r[1]} for r in p]),
            lambda p: execute_concurrent_with_args(self.ca, self.cass_insert_u, p),
            c_batch_params
        ))

        self.log_result("CREATE", "C3_Insert_Historia", *self.measure(
            lambda p: pg_c.execute("INSERT INTO historiaoperacji (id, urzadzenieid) VALUES (%s, %s) ON CONFLICT DO NOTHING", p),
            lambda p: my_c.execute("INSERT IGNORE INTO historiaoperacji (id, urzadzenieid) VALUES (%s, %s)", p),
            lambda p: self.mo["historiaoperacji"].insert_one({"id": p[0], "urzadzenieid": p[1]}),
            lambda p: self.ca.execute("INSERT INTO historiaoperacji (id, urzadzenieid) VALUES (%s, %s)", p),
            lambda: (random.randint(70_000_001, 80_000_000), 1)
        ))
        
        self.log_result("CREATE", "C4_Insert_Pracownik", *self.measure(
            lambda p: pg_c.execute("INSERT INTO pracownik (id, imie) VALUES (%s, %s) ON CONFLICT DO NOTHING", p),
            lambda p: my_c.execute("INSERT IGNORE INTO pracownik (id, imie) VALUES (%s, %s)", p),
            lambda p: self.mo["pracownik"].insert_one({"id": p[0], "imie": p[1]}),
            lambda p: self.ca.execute("INSERT INTO pracownik (id, imie) VALUES (%s, %s)", p),
            lambda: (random.randint(80_000_001, 90_000_000), "Jan")
        ))
        
        self.log_result("CREATE", "C5_Insert_Budynek", *self.measure(
            lambda p: pg_c.execute("INSERT INTO budynek (id) VALUES (%s) ON CONFLICT DO NOTHING", p),
            lambda p: my_c.execute("INSERT IGNORE INTO budynek (id) VALUES (%s)", p),
            lambda p: self.mo["budynek"].insert_one({"id": p[0]}),
            lambda p: self.ca.execute("INSERT INTO budynek (id) VALUES (%s)", p),
            lambda: (random.randint(90_000_001, 100_000_000),)
        ))

        def c6_batch_hist(): return [(random.randint(100_000_001, 110_000_000), 1) for _ in range(100)]
        self.log_result("CREATE", "C6_Batch_Hist_100", *self.measure(
            lambda p: psycopg2.extras.execute_batch(pg_c, "INSERT INTO historiaoperacji (id, urzadzenieid) VALUES (%s, %s) ON CONFLICT DO NOTHING", p),
            lambda p: my_c.executemany("INSERT IGNORE INTO historiaoperacji (id, urzadzenieid) VALUES (%s, %s)", p),
            lambda p: self.mo["historiaoperacji"].insert_many([{"id": r[0], "urzadzenieid": r[1]} for r in p]),
            None,
            c6_batch_hist
        ))

        # =====================================================================
        # 2. READ (Tutaj indeksy pokażą 1000x przyspieszenie!)
        # =====================================================================
        self.log_result("READ", "R1_Search_By_Nazwa", *self.measure(
            lambda p: pg_c.execute("SELECT * FROM urzadzenie WHERE nazwa = %s", p) or pg_c.fetchall(),
            lambda p: my_c.execute("SELECT * FROM urzadzenie WHERE nazwa = %s", p) or my_c.fetchall(),
            lambda p: list(self.mo["urzadzenie"].find({"nazwa": p[0]})),
            lambda p: list(self.ca.execute("SELECT * FROM urzadzenie WHERE nazwa = %s ALLOW FILTERING", p)),
            lambda: (f"Urzadzenie-{random.randint(1, s)}",)
        ))

        self.log_result("READ", "R2_Search_By_Stan", *self.measure(
            lambda p: pg_c.execute("SELECT * FROM urzadzenie WHERE stan = %s LIMIT 500", p) or pg_c.fetchall(),
            lambda p: my_c.execute("SELECT * FROM urzadzenie WHERE stan = %s LIMIT 500", p) or my_c.fetchall(),
            lambda p: list(self.mo["urzadzenie"].find({"stan": p[0]}).limit(500)),
            lambda p: list(self.ca.execute("SELECT * FROM urzadzenie WHERE stan = %s LIMIT 500 ALLOW FILTERING", p)),
            lambda: (random.choice(['Nowy', 'W naprawie', 'Zmagazynowany']),)
        ))

        self.log_result("READ", "R3_Date_Range_Sort", *self.measure(
            lambda p: pg_c.execute("SELECT * FROM historiaoperacji WHERE data_zdarzenia > %s ORDER BY data_zdarzenia DESC LIMIT 100", p) or pg_c.fetchall(),
            lambda p: my_c.execute("SELECT * FROM historiaoperacji WHERE data_zdarzenia > %s ORDER BY data_zdarzenia DESC LIMIT 100", p) or my_c.fetchall(),
            lambda p: list(self.mo["historiaoperacji"].find({"data_zdarzenia": {"$gt": p[0]}}).sort("data_zdarzenia", -1).limit(100)),
            None,
            lambda: (f"{random.randint(2021, 2023)}-01-01 00:00:00",)
        ))

        self.log_result("READ", "R4_Join_Urzadz_Model", *self.measure(
            lambda p: pg_c.execute("SELECT u.nazwa, m.nazwa FROM urzadzenie u JOIN model m ON u.modelid = m.id WHERE u.id = %s", p) or pg_c.fetchall(),
            lambda p: my_c.execute("SELECT u.nazwa, m.nazwa FROM urzadzenie u JOIN model m ON u.modelid = m.id WHERE u.id = %s", p) or my_c.fetchall(),
            lambda p: list(self.mo.urzadzenie.aggregate([{"$match": {"id": p[0]}}, {"$lookup": {"from": "model", "localField": "modelid", "foreignField": "id", "as": "m"}}])),
            None,
            lambda: (random.randint(1, s),)
        ))

        self.log_result("READ", "R5_Count_By_Stan", *self.measure(
            lambda p: pg_c.execute("SELECT COUNT(*) FROM urzadzenie WHERE stan = %s", p) or pg_c.fetchall(),
            lambda p: my_c.execute("SELECT COUNT(*) FROM urzadzenie WHERE stan = %s", p) or my_c.fetchall(),
            lambda p: self.mo["urzadzenie"].count_documents({"stan": p[0]}),
            None,
            lambda: (random.choice(['Nowy', 'W naprawie']),)
        ))

        self.log_result("READ", "R6_Read_By_PK", *self.measure(
            lambda p: pg_c.execute("SELECT * FROM urzadzenie WHERE id = %s", p) or pg_c.fetchall(),
            lambda p: my_c.execute("SELECT * FROM urzadzenie WHERE id = %s", p) or my_c.fetchall(),
            lambda p: self.mo["urzadzenie"].find_one({"id": p[0]}),
            lambda p: list(self.ca.execute("SELECT * FROM urzadzenie WHERE id = %s", p)),
            lambda: (random.randint(1, s),)
        ))

        # =====================================================================
        # 3. UPDATE (Szukanie rekordu zajmuje 99% czasu, zmiana 1%)
        # =====================================================================
        self.log_result("UPDATE", "U1_Mass_Update_Stan", *self.measure(
            lambda p: pg_c.execute("UPDATE urzadzenie SET stan = 'X' WHERE stan = %s", p),
            lambda p: my_c.execute("UPDATE urzadzenie SET stan = 'X' WHERE stan = %s", p),
            lambda p: self.mo["urzadzenie"].update_many({"stan": p[0]}, {"$set": {"stan": "X"}}),
            None, # Cassandra rzuci błędem bez indeksu
            lambda: (random.choice(['W użyciu', 'Zmagazynowany']),)
        ))

        self.log_result("UPDATE", "U2_Update_By_Nazwa", *self.measure(
            lambda p: pg_c.execute("UPDATE urzadzenie SET stan = 'Naprawa' WHERE nazwa = %s", p),
            lambda p: my_c.execute("UPDATE urzadzenie SET stan = 'Naprawa' WHERE nazwa = %s", p),
            lambda p: self.mo["urzadzenie"].update_one({"nazwa": p[0]}, {"$set": {"stan": "Naprawa"}}),
            None,
            lambda: (f"Urzadzenie-{random.randint(1, s)}",)
        ))

        self.log_result("UPDATE", "U3_Update_By_PK", *self.measure(
            lambda p: pg_c.execute("UPDATE urzadzenie SET stan = 'Y' WHERE id = %s", p),
            lambda p: my_c.execute("UPDATE urzadzenie SET stan = 'Y' WHERE id = %s", p),
            lambda p: self.mo["urzadzenie"].update_one({"id": p[0]}, {"$set": {"stan": "Y"}}),
            lambda p: self.ca.execute("UPDATE urzadzenie SET stan = 'Y' WHERE id = %s", p),
            lambda: (random.randint(1, s),)
        ))

        self.log_result("UPDATE", "U4_Update_Date_Range", *self.measure(
            lambda p: pg_c.execute("UPDATE historiaoperacji SET log = 'X' WHERE data_zdarzenia > %s", p),
            lambda p: my_c.execute("UPDATE historiaoperacji SET log = 'X' WHERE data_zdarzenia > %s", p),
            lambda p: self.mo["historiaoperacji"].update_many({"data_zdarzenia": {"$gt": p[0]}}, {"$set": {"log": "X"}}),
            None,
            lambda: ("2023-10-01 00:00:00",)
        ))

        self.log_result("UPDATE", "U5_Batch_PK_50", *self.measure(
            lambda p: psycopg2.extras.execute_batch(pg_c, "UPDATE urzadzenie SET stan = 'B' WHERE id = %s", p),
            lambda p: my_c.executemany("UPDATE urzadzenie SET stan = 'B' WHERE id = %s", p),
            None,
            None,
            lambda: [(random.randint(1, s),) for _ in range(50)]
        ))

        self.log_result("UPDATE", "U6_Update_FK", *self.measure(
            lambda p: pg_c.execute("UPDATE urzadzenie SET modelid = 2 WHERE przetargid = %s", p),
            lambda p: my_c.execute("UPDATE urzadzenie SET modelid = 2 WHERE przetargid = %s", p),
            lambda p: self.mo["urzadzenie"].update_many({"przetargid": p[0]}, {"$set": {"modelid": 2}}),
            None,
            lambda: (random.randint(1, 500),)
        ))

        # =====================================================================
        # 4. DELETE (Podobnie jak Update - wyszukanie determinuje czas)
        # =====================================================================
        self.log_result("DELETE", "D1_Delete_By_Nazwa", *self.measure(
            lambda p: pg_c.execute("DELETE FROM urzadzenie WHERE nazwa = %s", p),
            lambda p: my_c.execute("DELETE FROM urzadzenie WHERE nazwa = %s", p),
            lambda p: self.mo["urzadzenie"].delete_one({"nazwa": p[0]}),
            None,
            lambda: (f"Urzadzenie-DEL-{random.randint(1, s)}",)
        ))

        self.log_result("DELETE", "D2_Mass_Delete_Stan", *self.measure(
            lambda p: pg_c.execute("DELETE FROM urzadzenie WHERE stan = %s", p),
            lambda p: my_c.execute("DELETE FROM urzadzenie WHERE stan = %s", p),
            lambda p: self.mo["urzadzenie"].delete_many({"stan": p[0]}),
            None,
            lambda: ("Do usunięcia",)
        ))

        self.log_result("DELETE", "D3_Delete_By_PK", *self.measure(
            lambda p: pg_c.execute("DELETE FROM budynek WHERE id = %s", p),
            lambda p: my_c.execute("DELETE FROM budynek WHERE id = %s", p),
            lambda p: self.mo["budynek"].delete_one({"id": p[0]}),
            lambda p: self.ca.execute("DELETE FROM budynek WHERE id = %s", p),
            lambda: (random.randint(10**9, 2*10**9),)
        ))

        self.log_result("DELETE", "D4_Batch_Delete_50", *self.measure(
            lambda p: psycopg2.extras.execute_batch(pg_c, "DELETE FROM budynek WHERE id = %s", p),
            lambda p: my_c.executemany("DELETE FROM budynek WHERE id = %s", p),
            None, 
            lambda p: execute_concurrent_with_args(self.ca, self.cass_delete_budynek, p),
            lambda: [(random.randint(10**9, 2*10**9),) for _ in range(50)]
        ))

        self.log_result("DELETE", "D5_Delete_Date_Range", *self.measure(
            lambda p: pg_c.execute("DELETE FROM historiaoperacji WHERE data_zdarzenia < %s", p),
            lambda p: my_c.execute("DELETE FROM historiaoperacji WHERE data_zdarzenia < %s", p),
            lambda p: self.mo["historiaoperacji"].delete_many({"data_zdarzenia": {"$lt": p[0]}}),
            None,
            lambda: ("2019-01-01",)
        ))

        self.log_result("DELETE", "D6_Delete_By_FK", *self.measure(
            lambda p: pg_c.execute("DELETE FROM historiaoperacji WHERE pracownikid = %s", p),
            lambda p: my_c.execute("DELETE FROM historiaoperacji WHERE pracownikid = %s", p),
            lambda p: self.mo["historiaoperacji"].delete_many({"pracownikid": p[0]}),
            None,
            lambda: (random.randint(1, 5000),)
        ))

        # Zapis do pliku
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        fname = f"wyniki_{s}_{ts}.json"
        with open(fname, 'w') as f:
            json.dump(self.results, f, indent=4)
        print(f"\n[ZAKOŃCZONO] Zapisano raport do pliku: {fname}")
        
        self.pg.close()
        self.my.close()
        self.ca.cluster.shutdown()

if __name__ == "__main__":
    tester = DatabaseTester(CURRENT_SIZE)
    tester.run()