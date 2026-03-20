import os
import subprocess
import time

DATA_DIR = "data"

TABELE = [
    "budynek", "dzial", "producent", "karta", "przetarg", 
    "lokalizacja", "model", "pracownik", "urzadzenie", "historiaoperacji"
]

def uruchom_komende(komenda_lista, opis):
    print(f"[TRWA] {opis}...", end="", flush=True)
    wynik = subprocess.run(komenda_lista, capture_output=True, text=True, encoding='utf-8', errors='replace')
    if wynik.returncode == 0:
        print(" [OK]")
    else:
        blad = wynik.stderr.strip() if wynik.stderr.strip() else wynik.stdout.strip()
        print(f" [BŁĄD]\nSzczegóły: {blad[:200]}...")

def laduj_bazy():
    print("="*60)
    print(f" ROZPOCZYNAM ŁADOWANIE DANYCH Z FOLDERU: {DATA_DIR.upper()}")
    print("="*60)

    for tabela in TABELE:
        plik_csv = f"{tabela}.csv"
        sciezka_lokalna = os.path.join(DATA_DIR, plik_csv)
        
        if not os.path.exists(sciezka_lokalna):
            continue
            
        print(f"\n--- Przetwarzanie tabeli: {tabela.upper()} ---")

        # kopiowanie do kontenerów
        for kontener in ["it_postgres", "it_mysql", "it_mongo", "it_cassandra"]:
            uruchom_komende(["docker", "cp", sciezka_lokalna, f"{kontener}:/tmp/{plik_csv}"], f"Kopiowanie do {kontener}")

        # posgtres
        cmd_pg = ["docker", "exec", "it_postgres", "psql", "-U", "admin", "-d", "it_equipment", "-c",
                  f"\\copy {tabela} FROM '/tmp/{plik_csv}' WITH (FORMAT csv, HEADER true, DELIMITER ',');"]
        uruchom_komende(cmd_pg, "Import (Postgres)")

        # mysql zmienna globalna przed importem!!
        cmd_my_enable = ["docker", "exec", "it_mysql", "mysql", "-u", "admin", "-ppassword", "-e", "SET GLOBAL local_infile = 1;"]
        subprocess.run(cmd_my_enable, capture_output=True)

        cmd_my = [
            "docker", "exec", "it_mysql", "mysql", "-u", "admin", "-ppassword", "it_equipment", "--local-infile=1", "-e",
            f"LOAD DATA LOCAL INFILE '/tmp/{plik_csv}' INTO TABLE {tabela} FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\n' IGNORE 1 ROWS;"
        ]
        uruchom_komende(cmd_my, "Import (MySQL)")

        # mongo
        cmd_mongo = ["docker", "exec", "it_mongo", "mongoimport", "-u", "admin", "-p", "password", "--authenticationDatabase", "admin", 
                     "--db", "it_equipment", "--collection", tabela, "--type", "csv", "--headerline", "--file", f"/tmp/{plik_csv}"]
        uruchom_komende(cmd_mongo, "Import (MongoDB)")

        # casandra trzeba zmapowac przed bo bledy
        mapping = {
            "budynek": "(id, adres, oznaczenie)",
            "dzial": "(id, nazwa)", 
            "producent": "(id, nazwa)",
            "karta": "(id, numer)",
            "przetarg": "(id, numerumowy, datarozpoczecia, datazakonczenia)",
            "lokalizacja": "(id, pokoj, budynekid)",
            "model": "(id, nazwa, producentid)",
            "pracownik": "(id, imie, nazwisko, dzialid, kartaid)",
            "urzadzenie": "(id, nazwa, przetargid, numerseryjny, stan, modelid)",
            "historiaoperacji": "(id, urzadzenieid, pracownikid, lokalizacjaid, data_zdarzenia, log)"
        }

        cols = mapping.get(tabela, "")
        
        cmd_cass = [
            "docker", "exec", "it_cassandra", "cqlsh", 
            "--request-timeout=3600",
            "-e", f"USE it_equipment; COPY {tabela} {cols} FROM '/tmp/{plik_csv}' WITH HEADER = TRUE AND DELIMITER = ',' AND MAXATTEMPTS=10;"
        ]
        uruchom_komende(cmd_cass, "Import (Cassandra)")

if __name__ == "__main__":
    laduj_bazy()