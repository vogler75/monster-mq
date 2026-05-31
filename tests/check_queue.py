#!/usr/bin/env python3
import psycopg2
import json

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
        
        # Select latest 10 rows from kafka_queue_monster1
        cur.execute("SELECT offset_id, topic, payload, creation_time, publisher_id FROM kafka_queue_monster1 ORDER BY offset_id DESC LIMIT 10;")
        rows = cur.fetchall()
        
        print("\n=== Latest 10 Queue Messages in PostgreSQL ===")
        print(f"{'Offset ID':<10} | {'Topic Column (Kafka Key)':<40} | {'Payload':<30} | {'Publisher ID'}")
        print("-" * 110)
        for row in rows:
            payload_str = row[2].tobytes().decode('utf-8', errors='ignore') if row[2] else "None"
            # truncate payload
            if len(payload_str) > 30:
                payload_str = payload_str[:27] + "..."
            print(f"{row[0]:<10} | {row[1]:<40} | {payload_str:<30} | {row[4]}")
        print("==============================================\n")
        
        cur.close()
    except Exception as e:
        print(f"Error querying database: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
