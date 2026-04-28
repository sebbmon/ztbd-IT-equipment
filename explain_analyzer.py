import psycopg2
import mysql.connector
from pymongo import MongoClient
import json

def connect_all():
    print("Nawiązywanie połączeń z bazami do analizy EXPLAIN...\n")
    pg = psycopg2.connect(host="localhost", port=5432, user="admin", password="password", dbname="it_equipment")
    pg.autocommit = True
    my = mysql.connector.connect(host="localhost", port=3306, user="admin", password="password", database="it_equipment")
    my.autocommit = True
    mongo = MongoClient("mongodb://admin:password@localhost:27017/?authSource=admin")["it_equipment"]
    
    # Cassandra nie posiada instrukcji EXPLAIN - korzysta z Tracingu, który działa zupełnie inaczej
    return pg, my, mongo

def analyze_postgres(pg, query):
    print("="*60)
    print(" POSTGRESQL - EXPLAIN ANALYZE")
    print("="*60)
    try:
        with pg.cursor() as cur:
            # EXPLAIN ANALYZE faktycznie wykonuje zapytanie i mierzy czas
            cur.execute(f"EXPLAIN ANALYZE {query}")
            for row in cur.fetchall():
                print(row[0])
    except Exception as e:
        print(f"Błąd PostgreSQL: {e}")

def analyze_mysql(my, query):
    print("\n" + "="*60)
    print(" MYSQL - EXPLAIN FORMAT=JSON")
    print("="*60)
    try:
        with my.cursor() as cur:
            # FORMAT=JSON daje dokładne statystyki, m.in. 'rows_examined'
            cur.execute(f"EXPLAIN FORMAT=JSON {query}")
            result = cur.fetchall()
            if result:
                # Parsujemy JSON
                parsed = json.loads(result[0][0])
                print(json.dumps(parsed, indent=2))
    except Exception as e:
        print(f"Błąd MySQL: {e}")

def analyze_mongo(mongo, query_dict):
    print("\n" + "="*60)
    print(" MONGODB - explain('executionStats')")
    print("="*60)
    try:
        # Zlecamy Mongo wykonanie planu (executionStats wykonuje fizycznie zapytanie)
        explain_result = mongo.command(
            "explain", 
            {"find": "historiaoperacji", "filter": query_dict, "sort": {"data_zdarzenia": -1}, "limit": 100}, 
            verbosity="executionStats"
        )
        
        # Wyciągamy tylko najważniejsze dane dla czytelności
        stats = explain_result.get("executionStats", {})
        plan = explain_result.get("queryPlanner", {}).get("winningPlan", {})
        
        print(f"Zwycięski plan (Stage): {plan.get('stage')}")
        if plan.get('stage') == 'FETCH':
            print(f"Pod-plan (Input Stage): {plan.get('inputStage', {}).get('stage')}")
            
        print(f"Przeszukane dokumenty (totalDocsExamined): {stats.get('totalDocsExamined')}")
        print(f"Przeszukane klucze indeksu (totalKeysExamined): {stats.get('totalKeysExamined')}")
        print(f"Zwrócone dokumenty (nReturned): {stats.get('nReturned')}")
        print(f"Czas wykonania (executionTimeMillis): {stats.get('executionTimeMillis')} ms")
        
    except Exception as e:
        print(f"Błąd MongoDB: {e}")

if __name__ == "__main__":
    pg, my, mongo = connect_all()
    
    # Zapytanie analityczne używane w teście R3 - szukanie po dacie
    sql_query = "SELECT * FROM historiaoperacji WHERE data_zdarzenia > '2023-01-01 00:00:00' ORDER BY data_zdarzenia DESC LIMIT 100;"
    mongo_query = {"data_zdarzenia": {"$gt": "2023-01-01 00:00:00"}}
    
    print(f"ZAPYTANIE TESTOWE: Szukanie w tabeli 'historiaoperacji' po dacie (> 2023)\n")
    
    analyze_postgres(pg, sql_query)
    analyze_mysql(my, sql_query)
    analyze_mongo(mongo, mongo_query)
    
    pg.close()
    my.close()