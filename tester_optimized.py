import time
import psycopg2
import mysql.connector
from pymongo import MongoClient
from cassandra.cluster import Cluster

# KONFIGURACJA I POŁĄCZENIA
def connect_all():
    pg = psycopg2.connect(host="localhost", port=5432, user="admin", password="password", dbname="it_equipment")
    pg.autocommit = True
    
    my = mysql.connector.connect(host="localhost", port=3306, user="admin", password="password", database="it_equipment")
    my.autocommit = True
    
    mongo = MongoClient("mongodb://admin:password@localhost:27017/?authSource=admin")["it_equipment"]
    
    cluster = Cluster(['localhost'], port=9042)
    cass = cluster.connect('it_equipment')
    cass.default_timeout = 60.0 
    
    return pg, my, mongo, cass

# NARZĘDZIA POMIAROWE I OPTYMALIZACJA
def clear_db_cache(db_name, pg, my, mongo):
    """Próba wymuszenia czyszczenia cache zapytań i tabel w silnikach bazodanowych."""
    try:
        if db_name == "Postgres":
            with pg.cursor() as cur:
                cur.execute("DISCARD ALL;") # Czyści plany zapytań i stan sesji
        elif db_name == "MySQL":
            with my.cursor() as cur:
                cur.execute("FLUSH TABLES;") # Zamyka tabele i zrzuca cache w MySQL
        elif db_name == "Mongo":
            # Czyści cache planów wykonania zapytań dla używanych kolekcji
            for coll in ["budynek", "dzial", "producent", "karta", "pracownik", "urzadzenie", "przetarg", "lokalizacja"]:
                mongo.command("planCacheClear", coll)
        # Cassandra zarządza cache na poziomie JVM, nie da się go łatwo zrzucić komendą CQL z poziomu Pythona.
    except Exception:
        pass

def my_execute_read(cur, sql):
    """Zoptymalizowany odczyt MySQL bez tworzenia kursora na nowo."""
    cur.execute(sql)
    if cur.description is not None:
        cur.fetchall()

def pg_execute_read(cur, sql):
    """Zoptymalizowany odczyt Postgres bez tworzenia kursora na nowo."""
    cur.execute(sql)
    if cur.description is not None:
        cur.fetchall()

def measure(db_name, func, pg, my, mongo):
    times = []
    for _ in range(3):
        # Wyczyszczenie cache PRZED uruchomieniem stopera
        clear_db_cache(db_name, pg, my, mongo)
        
        try:
            start = time.perf_counter()
            func()
            times.append((time.perf_counter() - start) * 1000)
        except Exception as e:
            print(f"   [{db_name.ljust(9)}] BŁĄD: {str(e).strip().splitlines()[0]}")
            return
            
    avg = sum(times) / 3
    print(f"   [{db_name.ljust(9)}] Średni czas: {avg:8.2f} ms")

# ==========================================
# 24 SCENARIUSZE TESTOWE
# ==========================================
def run_tests(pg, my, mongo, cass):
    # Tworzymy stałe kursory, aby nie obciążać stopera ich tworzeniem w lambdach
    pg_cur = pg.cursor()
    my_cur = my.cursor(buffered=True)

    print("\n" + "="*50)
    print(" ROZPOCZYNAMY TESTY: CREATE")
    print("="*50)

    print("\n[C1] Dodanie nowego budynku (ID: 999901)")
    measure("Postgres", lambda: pg_cur.execute("INSERT INTO budynek (id, adres, oznaczenie) VALUES (999901, 'Testowa 1', 'B1') ON CONFLICT DO NOTHING;"), pg, my, mongo)
    measure("MySQL", lambda: my_cur.execute("INSERT IGNORE INTO budynek (id, adres, oznaczenie) VALUES (999901, 'Testowa 1', 'B1');"), pg, my, mongo)
    measure("Mongo", lambda: mongo["budynek"].insert_one({"id": 999901, "adres": "Testowa 1", "oznaczenie": "B1"}) if not mongo["budynek"].find_one({"id": 999901}) else None, pg, my, mongo)
    measure("Cassandra", lambda: cass.execute("INSERT INTO budynek (id, adres, oznaczenie) VALUES (999901, 'Testowa 1', 'B1');"), pg, my, mongo)

    print("\n[C2] Dodanie nowego działu (ID: 999901)")
    measure("Postgres", lambda: pg_cur.execute("INSERT INTO dzial (id, nazwa) VALUES (999901, 'Dzial Testowy') ON CONFLICT DO NOTHING;"), pg, my, mongo)
    measure("MySQL", lambda: my_cur.execute("INSERT IGNORE INTO dzial (id, nazwa) VALUES (999901, 'Dzial Testowy');"), pg, my, mongo)
    measure("Mongo", lambda: mongo["dzial"].insert_one({"id": 999901, "nazwa": "Dzial Testowy"}) if not mongo["dzial"].find_one({"id": 999901}) else None, pg, my, mongo)
    measure("Cassandra", lambda: cass.execute("INSERT INTO dzial (id, nazwa) VALUES (999901, 'Dzial Testowy');"), pg, my, mongo)

    print("\n[C3] Dodanie nowego producenta (ID: 999901)")
    measure("Postgres", lambda: pg_cur.execute("INSERT INTO producent (id, nazwa) VALUES (999901, 'Producent Testowy') ON CONFLICT DO NOTHING;"), pg, my, mongo)
    measure("MySQL", lambda: my_cur.execute("INSERT IGNORE INTO producent (id, nazwa) VALUES (999901, 'Producent Testowy');"), pg, my, mongo)
    measure("Mongo", lambda: mongo["producent"].insert_one({"id": 999901, "nazwa": "Producent Testowy"}) if not mongo["producent"].find_one({"id": 999901}) else None, pg, my, mongo)
    measure("Cassandra", lambda: cass.execute("INSERT INTO producent (id, nazwa) VALUES (999901, 'Producent Testowy');"), pg, my, mongo)

    print("\n[C4] Wydanie nowej karty dostępowej (ID: 999901)")
    measure("Postgres", lambda: pg_cur.execute("INSERT INTO karta (id, numer) VALUES (999901, 'KARTA-999') ON CONFLICT DO NOTHING;"), pg, my, mongo)
    measure("MySQL", lambda: my_cur.execute("INSERT IGNORE INTO karta (id, numer) VALUES (999901, 'KARTA-999');"), pg, my, mongo)
    measure("Mongo", lambda: mongo["karta"].insert_one({"id": 999901, "numer": "KARTA-999"}) if not mongo["karta"].find_one({"id": 999901}) else None, pg, my, mongo)
    measure("Cassandra", lambda: cass.execute("INSERT INTO karta (id, numer) VALUES (999901, 'KARTA-999');"), pg, my, mongo)

    print("\n[C5] Zatrudnienie pracownika testowego (ID: 999901)")
    measure("Postgres", lambda: pg_cur.execute("INSERT INTO pracownik (id, imie, nazwisko, dzialid, kartaid) VALUES (999901, 'Jan', 'Testowy', 999901, 999901) ON CONFLICT DO NOTHING;"), pg, my, mongo)
    measure("MySQL", lambda: my_cur.execute("INSERT IGNORE INTO pracownik (id, imie, nazwisko, dzialid, kartaid) VALUES (999901, 'Jan', 'Testowy', 999901, 999901);"), pg, my, mongo)
    measure("Mongo", lambda: mongo["pracownik"].insert_one({"id": 999901, "imie": "Jan", "nazwisko": "Testowy", "dzialid": 999901, "kartaid": 999901}) if not mongo["pracownik"].find_one({"id": 999901}) else None, pg, my, mongo)
    measure("Cassandra", lambda: cass.execute("INSERT INTO pracownik (id, imie, nazwisko, dzialid, kartaid) VALUES (999901, 'Jan', 'Testowy', 999901, 999901);"), pg, my, mongo)

    print("\n[C6] Wprowadzenie urządzenia testowego (ID: 999901)")
    measure("Postgres", lambda: pg_cur.execute("INSERT INTO urzadzenie (id, nazwa, przetargid, numerseryjny, stan, modelid) VALUES (999901, 'Urzadzenie-Test', 1, 'SN-TEST-1', 'Nowy', 1) ON CONFLICT DO NOTHING;"), pg, my, mongo)
    measure("MySQL", lambda: my_cur.execute("INSERT IGNORE INTO urzadzenie (id, nazwa, przetargid, numerseryjny, stan, modelid) VALUES (999901, 'Urzadzenie-Test', 1, 'SN-TEST-1', 'Nowy', 1);"), pg, my, mongo)
    measure("Mongo", lambda: mongo["urzadzenie"].insert_one({"id": 999901, "nazwa": "Urzadzenie-Test", "przetargid": 1, "numerseryjny": "SN-TEST-1", "stan": "Nowy", "modelid": 1}) if not mongo["urzadzenie"].find_one({"id": 999901}) else None, pg, my, mongo)
    measure("Cassandra", lambda: cass.execute("INSERT INTO urzadzenie (id, nazwa, przetargid, numerseryjny, stan, modelid) VALUES (999901, 'Urzadzenie-Test', 1, 'SN-TEST-1', 'Nowy', 1);"), pg, my, mongo)

    print("\n" + "="*50)
    print(" ROZPOCZYNAMY TESTY: READ")
    print("="*50)

    print("\n[R1] Wyszukanie urządzenia po ID = 100")
    measure("Postgres", lambda: pg_execute_read(pg_cur, "SELECT * FROM urzadzenie WHERE id = 100;"), pg, my, mongo)
    measure("MySQL", lambda: my_execute_read(my_cur, "SELECT * FROM urzadzenie WHERE id = 100;"), pg, my, mongo)
    measure("Mongo", lambda: mongo["urzadzenie"].find_one({"id": 100}), pg, my, mongo)
    measure("Cassandra", lambda: cass.execute("SELECT * FROM urzadzenie WHERE id = 100;"), pg, my, mongo)

    print("\n[R2] Wyszukanie urządzenia po zaindeksowanej nazwie ('Urzadzenie-50000')")
    measure("Postgres", lambda: pg_execute_read(pg_cur, "SELECT * FROM urzadzenie WHERE nazwa = 'Urzadzenie-50000';"), pg, my, mongo)
    measure("MySQL", lambda: my_execute_read(my_cur, "SELECT * FROM urzadzenie WHERE nazwa = 'Urzadzenie-50000';"), pg, my, mongo)
    measure("Mongo", lambda: list(mongo["urzadzenie"].find({"nazwa": "Urzadzenie-50000"})), pg, my, mongo)
    measure("Cassandra", lambda: cass.execute("SELECT * FROM urzadzenie WHERE nazwa = 'Urzadzenie-50000';"), pg, my, mongo)

    print("\n[R3] Sprawdzenie danych pracownika po ID = 100")
    measure("Postgres", lambda: pg_execute_read(pg_cur, "SELECT * FROM pracownik WHERE id = 100;"), pg, my, mongo)
    measure("MySQL", lambda: my_execute_read(my_cur, "SELECT * FROM pracownik WHERE id = 100;"), pg, my, mongo)
    measure("Mongo", lambda: mongo["pracownik"].find_one({"id": 100}), pg, my, mongo)
    measure("Cassandra", lambda: cass.execute("SELECT * FROM pracownik WHERE id = 100;"), pg, my, mongo)

    print("\n[R4] Odczytanie szczegółów przetargu ID = 10")
    measure("Postgres", lambda: pg_execute_read(pg_cur, "SELECT * FROM przetarg WHERE id = 10;"), pg, my, mongo)
    measure("MySQL", lambda: my_execute_read(my_cur, "SELECT * FROM przetarg WHERE id = 10;"), pg, my, mongo)
    measure("Mongo", lambda: mongo["przetarg"].find_one({"id": 10}), pg, my, mongo)
    measure("Cassandra", lambda: cass.execute("SELECT * FROM przetarg WHERE id = 10;"), pg, my, mongo)

    print("\n[R5] Pobranie danych producenta ID = 5")
    measure("Postgres", lambda: pg_execute_read(pg_cur, "SELECT * FROM producent WHERE id = 5;"), pg, my, mongo)
    measure("MySQL", lambda: my_execute_read(my_cur, "SELECT * FROM producent WHERE id = 5;"), pg, my, mongo)
    measure("Mongo", lambda: mongo["producent"].find_one({"id": 5}), pg, my, mongo)
    measure("Cassandra", lambda: cass.execute("SELECT * FROM producent WHERE id = 5;"), pg, my, mongo)

    print("\n[R6] Odczyt lokalizacji ID = 10")
    measure("Postgres", lambda: pg_execute_read(pg_cur, "SELECT * FROM lokalizacja WHERE id = 10;"), pg, my, mongo)
    measure("MySQL", lambda: my_execute_read(my_cur, "SELECT * FROM lokalizacja WHERE id = 10;"), pg, my, mongo)
    measure("Mongo", lambda: mongo["lokalizacja"].find_one({"id": 10}), pg, my, mongo)
    measure("Cassandra", lambda: cass.execute("SELECT * FROM lokalizacja WHERE id = 10;"), pg, my, mongo)

    print("\n" + "="*50)
    print(" ROZPOCZYNAMY TESTY: UPDATE")
    print("="*50)

    print("\n[U1] Zmiana statusu testowego urządzenia na 'W użyciu'")
    measure("Postgres", lambda: pg_cur.execute("UPDATE urzadzenie SET stan = 'W użyciu' WHERE id = 999901;"), pg, my, mongo)
    measure("MySQL", lambda: my_cur.execute("UPDATE urzadzenie SET stan = 'W użyciu' WHERE id = 999901;"), pg, my, mongo)
    measure("Mongo", lambda: mongo["urzadzenie"].update_one({"id": 999901}, {"$set": {"stan": "W użyciu"}}), pg, my, mongo)
    measure("Cassandra", lambda: cass.execute("UPDATE urzadzenie SET stan = 'W użyciu' WHERE id = 999901;"), pg, my, mongo)

    print("\n[U2] Aktualizacja adresu testowego budynku")
    measure("Postgres", lambda: pg_cur.execute("UPDATE budynek SET adres = 'Nowy Adres 2' WHERE id = 999901;"), pg, my, mongo)
    measure("MySQL", lambda: my_cur.execute("UPDATE budynek SET adres = 'Nowy Adres 2' WHERE id = 999901;"), pg, my, mongo)
    measure("Mongo", lambda: mongo["budynek"].update_one({"id": 999901}, {"$set": {"adres": "Nowy Adres 2"}}), pg, my, mongo)
    measure("Cassandra", lambda: cass.execute("UPDATE budynek SET adres = 'Nowy Adres 2' WHERE id = 999901;"), pg, my, mongo)

    print("\n[U3] Zmiana nazwy testowego działu")
    measure("Postgres", lambda: pg_cur.execute("UPDATE dzial SET nazwa = 'Dzial Super Testowy' WHERE id = 999901;"), pg, my, mongo)
    measure("MySQL", lambda: my_cur.execute("UPDATE dzial SET nazwa = 'Dzial Super Testowy' WHERE id = 999901;"), pg, my, mongo)
    measure("Mongo", lambda: mongo["dzial"].update_one({"id": 999901}, {"$set": {"nazwa": "Dzial Super Testowy"}}), pg, my, mongo)
    measure("Cassandra", lambda: cass.execute("UPDATE dzial SET nazwa = 'Dzial Super Testowy' WHERE id = 999901;"), pg, my, mongo)

    print("\n[U4] Zmiana nazwiska testowego pracownika")
    measure("Postgres", lambda: pg_cur.execute("UPDATE pracownik SET nazwisko = 'Kowalski' WHERE id = 999901;"), pg, my, mongo)
    measure("MySQL", lambda: my_cur.execute("UPDATE pracownik SET nazwisko = 'Kowalski' WHERE id = 999901;"), pg, my, mongo)
    measure("Mongo", lambda: mongo["pracownik"].update_one({"id": 999901}, {"$set": {"nazwisko": "Kowalski"}}), pg, my, mongo)
    measure("Cassandra", lambda: cass.execute("UPDATE pracownik SET nazwisko = 'Kowalski' WHERE id = 999901;"), pg, my, mongo)

    print("\n[U5] Aktualizacja nazwy producenta")
    measure("Postgres", lambda: pg_cur.execute("UPDATE producent SET nazwa = 'Nowy Producent' WHERE id = 999901;"), pg, my, mongo)
    measure("MySQL", lambda: my_cur.execute("UPDATE producent SET nazwa = 'Nowy Producent' WHERE id = 999901;"), pg, my, mongo)
    measure("Mongo", lambda: mongo["producent"].update_one({"id": 999901}, {"$set": {"nazwa": "Nowy Producent"}}), pg, my, mongo)
    measure("Cassandra", lambda: cass.execute("UPDATE producent SET nazwa = 'Nowy Producent' WHERE id = 999901;"), pg, my, mongo)

    print("\n[U6] Zmiana numeru seryjnego testowego urządzenia")
    measure("Postgres", lambda: pg_cur.execute("UPDATE urzadzenie SET numerseryjny = 'SN-NEW-99' WHERE id = 999901;"), pg, my, mongo)
    measure("MySQL", lambda: my_cur.execute("UPDATE urzadzenie SET numerseryjny = 'SN-NEW-99' WHERE id = 999901;"), pg, my, mongo)
    measure("Mongo", lambda: mongo["urzadzenie"].update_one({"id": 999901}, {"$set": {"numerseryjny": "SN-NEW-99"}}), pg, my, mongo)
    measure("Cassandra", lambda: cass.execute("UPDATE urzadzenie SET numerseryjny = 'SN-NEW-99' WHERE id = 999901;"), pg, my, mongo)

    print("\n" + "="*50)
    print(" ROZPOCZYNAM TESTY: DELETE (Usuwanie)")
    print("="*50)

    print("\n[D1] Złomowanie testowego urządzenia (ID: 999901)")
    measure("Postgres", lambda: pg_cur.execute("DELETE FROM urzadzenie WHERE id = 999901;"), pg, my, mongo)
    measure("MySQL", lambda: my_cur.execute("DELETE FROM urzadzenie WHERE id = 999901;"), pg, my, mongo)
    measure("Mongo", lambda: mongo["urzadzenie"].delete_one({"id": 999901}), pg, my, mongo)
    measure("Cassandra", lambda: cass.execute("DELETE FROM urzadzenie WHERE id = 999901;"), pg, my, mongo)

    print("\n[D2] Usunięcie testowego pracownika (ID: 999901)")
    measure("Postgres", lambda: pg_cur.execute("DELETE FROM pracownik WHERE id = 999901;"), pg, my, mongo)
    measure("MySQL", lambda: my_cur.execute("DELETE FROM pracownik WHERE id = 999901;"), pg, my, mongo)
    measure("Mongo", lambda: mongo["pracownik"].delete_one({"id": 999901}), pg, my, mongo)
    measure("Cassandra", lambda: cass.execute("DELETE FROM pracownik WHERE id = 999901;"), pg, my, mongo)

    print("\n[D3] Usunięcie testowej karty dostępowej (ID: 999901)")
    measure("Postgres", lambda: pg_cur.execute("DELETE FROM karta WHERE id = 999901;"), pg, my, mongo)
    measure("MySQL", lambda: my_cur.execute("DELETE FROM karta WHERE id = 999901;"), pg, my, mongo)
    measure("Mongo", lambda: mongo["karta"].delete_one({"id": 999901}), pg, my, mongo)
    measure("Cassandra", lambda: cass.execute("DELETE FROM karta WHERE id = 999901;"), pg, my, mongo)

    print("\n[D4] Usunięcie testowego producenta (ID: 999901)")
    measure("Postgres", lambda: pg_cur.execute("DELETE FROM producent WHERE id = 999901;"), pg, my, mongo)
    measure("MySQL", lambda: my_cur.execute("DELETE FROM producent WHERE id = 999901;"), pg, my, mongo)
    measure("Mongo", lambda: mongo["producent"].delete_one({"id": 999901}), pg, my, mongo)
    measure("Cassandra", lambda: cass.execute("DELETE FROM producent WHERE id = 999901;"), pg, my, mongo)

    print("\n[D5] Usunięcie testowego działu (ID: 999901)")
    measure("Postgres", lambda: pg_cur.execute("DELETE FROM dzial WHERE id = 999901;"), pg, my, mongo)
    measure("MySQL", lambda: my_cur.execute("DELETE FROM dzial WHERE id = 999901;"), pg, my, mongo)
    measure("Mongo", lambda: mongo["dzial"].delete_one({"id": 999901}), pg, my, mongo)
    measure("Cassandra", lambda: cass.execute("DELETE FROM dzial WHERE id = 999901;"), pg, my, mongo)

    print("\n[D6] Wyburzenie testowego budynku (ID: 999901)")
    measure("Postgres", lambda: pg_cur.execute("DELETE FROM budynek WHERE id = 999901;"), pg, my, mongo)
    measure("MySQL", lambda: my_cur.execute("DELETE FROM budynek WHERE id = 999901;"), pg, my, mongo)
    measure("Mongo", lambda: mongo["budynek"].delete_one({"id": 999901}), pg, my, mongo)
    measure("Cassandra", lambda: cass.execute("DELETE FROM budynek WHERE id = 999901;"), pg, my, mongo)

    pg_cur.close()
    my_cur.close()

# GŁÓWNA PĘTLA
if __name__ == "__main__":
    print("Nawiązywanie połączeń z bazami...")
    pg, my, mongo, cass = connect_all()
    print("Połączenia nawiązane poprawnie!")
    
    run_tests(pg, my, mongo, cass)
    
    pg.close()
    my.close()
    cass.cluster.shutdown()
    print("\n==================================================")
    print(" TESTY ZAKOŃCZONE")
    print("==================================================")