#!/usr/bin/env python3
import psycopg2

def main():
    conn = None
    try:
        # Connect to Postgres database
        conn = psycopg2.connect(
            host="192.168.1.35",
            port=5432,
            database="monster",
            user="system",
            password="manager"
        )
        cur = conn.cursor()
        
        # Query kafka_offsets_monster1
        cur.execute("SELECT group_id, topic, partition_id, committed_offset, last_commit_time FROM kafka_offsets_monster1;")
        rows = cur.fetchall()
        
        print("\n=== Committed Kafka Offsets in PostgreSQL ===")
        print(f"{'Group ID':<15} | {'Topic':<15} | {'Partition':<10} | {'Offset':<10} | {'Last Commit Time'}")
        print("-" * 80)
        if not rows:
            print("No offsets committed yet.")
        for row in rows:
            print(f"{row[0]:<15} | {row[1]:<15} | {row[2]:<10} | {row[3]:<10} | {row[4]}")
        print("=============================================\n")
        
        cur.close()
    except Exception as e:
        print(f"Error querying database: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
