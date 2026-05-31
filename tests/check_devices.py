#!/usr/bin/env python3
import psycopg2

def main():
    conn = None
    try:
        conn = psycopg2.connect(
            host="192.168.1.35",
            port=5432,
            database="monster",
            user="system",
            password="manager"
        )
        cur = conn.cursor()
        cur.execute("SET search_path TO system, public;")
        cur.execute("SELECT name, namespace, type, config FROM deviceconfigs WHERE type = 'KafkaStream';")
        rows = cur.fetchall()
        
        print("\n=== Dynamic Kafka Streams in PostgreSQL ===")
        for row in rows:
            print(f"Name: {row[0]:<15} | Namespace: {row[1]:<15} | Config: {row[2]}")
        print("===========================================\n")
        
        cur.close()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
