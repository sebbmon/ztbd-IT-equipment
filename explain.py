import subprocess
import os

OUTPUT_FILE = "explain_wyniki.txt"

# zapytanie
TEST_QUERY_BASE = "SELECT * FROM urzadzenie WHERE nazwa = 'Urzadzenie-50000'"

def run_command(command, db_name, file):
    print(f"Pobieranie planu zapytania z: {db_name}...")
    file.write(f"\n{'='*60}\n")
    file.write(f" BAZA DANYCH: {db_name.upper()}\n")
    file.write(f"{'='*60}\n\n")
    
    result = subprocess.run(command, shell=True, capture_output=True, text=True, encoding='utf-8', errors='replace')
    
    if result.returncode == 0:
        file.write(result.stdout)
    else:
        file.write(f"[BŁĄD] Nie udało się pobrać danych:\nSTDERR: {result.stderr}\nSTDOUT: {result.stdout}\n")

def generate_explains():
    print("Rozpoczynam generowanie planów zapytań (EXPLAIN)...")
    
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("============================================================\n")
        f.write(" ANALIZA PLANÓW ZAPYTAŃ (EXPLAIN)\n")
        f.write(" Zapytanie: Wyszukiwanie urządzenia po nazwie\n")
        f.write("============================================================\n")

        # 1. postgres
        cmd_pg = f'docker exec it_postgres psql -U admin -d it_equipment -c "EXPLAIN ANALYZE {TEST_QUERY_BASE};"'
        run_command(cmd_pg, "PostgreSQL", f)

        # 2. mysql
        cmd_my = f'docker exec it_mysql mysql -u admin -ppassword it_equipment -t -e "EXPLAIN {TEST_QUERY_BASE};"'
        run_command(cmd_my, "MySQL", f)

        # 3. mongo
        cmd_mongo = f'''docker exec it_mongo mongosh -u admin -p password --authenticationDatabase admin it_equipment --quiet --eval "JSON.stringify(db.urzadzenie.find({{nazwa: 'Urzadzenie-50000'}}).explain('executionStats'), null, 2)"'''
        run_command(cmd_mongo, "MongoDB", f)

        # 4. cassandra
        cmd_cass = f'docker exec it_cassandra cqlsh -e "USE it_equipment; TRACING ON; {TEST_QUERY_BASE} ALLOW FILTERING;"'
        run_command(cmd_cass, "Cassandra", f)

    print(f"\nWyniki zostały zapisane w pliku: {OUTPUT_FILE}")

if __name__ == "__main__":
    generate_explains()