import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import random
import string
from datetime import datetime
from pymongo import MongoClient, UpdateOne, ASCENDING

DATABASE_TYPE = 1
BATCH_SIZE = 1000


def generate_dummy_payload():
    # Randomly determine the size between a and b characters
    size = random.randint(10, 1000)

    # Generate a random string of the determined size
    random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=size))

    # Convert the string to a binary payload (bytes)
    binary_payload = random_string.encode('utf-8')

    return binary_payload


def create_test_data():
    # Database connection parameters
    if DATABASE_TYPE == 1:
        conn = psycopg2.connect(
            dbname="postgres",
            user="system",
            password="manager",
            host="linux0",
            port="5432"
        )
        cursor = conn.cursor()

    if DATABASE_TYPE == 2:
        client = MongoClient("mongodb://system:manager@linux0:27017/")
        db = client['mqtt']
        try:
            # Create the collection (optional, MongoDB creates it automatically on first insert)
            collection = db.create_collection('retained', capped=False)  # Remove capped=False if you want to use capped collection

            # Create an index on the "levels" field
            collection.create_index([('levels', ASCENDING)], name='levels_index')
        except Exception as e:
            print(e)

        collection = db["retained"]

    try:
        message_counter = 0
        topic_nr1 = 0
        topic_nr2 = 0
        topic_nr3 = 0
        topic_nr4 = 0
        topic_nr5 = 0
        topic_nr6 = 0

        records = []
        topics = set()
        topics.clear()
        while message_counter < 10_000_000:
            topic_nr1 = random.randint(0, 10)
            topic_nr2 = random.randint(0, 10)
            topic_nr3 = random.randint(0, 10)
            topic_nr4 = random.randint(0, 10)
            topic_nr5 = random.randint(0, 10)
            topic_nr6 = random.randint(0, 100)

            """
            topic_nr6 = topic_nr6 + 1
            if topic_nr6 == 100:
                topic_nr6 = 0
                topic_nr5 = topic_nr5 + 1
                if topic_nr5 == 10:
                    topic_nr5 = 0
                    topic_nr4 = topic_nr4 + 1
                    if topic_nr4 == 10:
                        topic_nr4 = 0
                        topic_nr3 = topic_nr3 + 1
                        if topic_nr3 == 10:
                            topic_nr3 = 0
                            topic_nr2 = topic_nr2 + 1
                            if topic_nr2 == 10:
                                topic_nr2 = 0
                                topic_nr1 = topic_nr1 + 1
                                if topic_nr1 == 10:
                                    topic_nr1 = 0
            """

            levels = ["enterprise_" + str(topic_nr1),
                      "site_" + str(topic_nr2),
                      "area_" + str(topic_nr3),
                      "line_" + str(topic_nr4),
                      "cell_" + str(topic_nr5),
                      "property_" + str(topic_nr6)]  # str(uuid.uuid4())
            topic = "/".join(levels)

            if topic in topics:
                continue

            message_counter = message_counter + 1
            payload = generate_dummy_payload()  # datetime.now().isoformat().encode('utf-8')

            topics.add(topic)

            if DATABASE_TYPE == 1:
                records.append((levels,))
                #records.append((topic, levels, payload))
            if DATABASE_TYPE == 2:
                records.append(UpdateOne(
                    {"topic": topic},
                    {"$set": {"levels": levels, "payload": payload}},
                    upsert=True
                ))

            if len(records) >= BATCH_SIZE:
                if DATABASE_TYPE == 1:
                    #s = """
                    #    INSERT INTO retained (topic, levels, payload) VALUES %s
                    #    ON CONFLICT (topic) DO UPDATE SET payload = EXCLUDED.payload
                    #    """
                    s = """
                        INSERT INTO retained (levels) VALUES %s
                        ON CONFLICT (levels) DO NOTHING
                        """
                    execute_values(
                        cursor,s,
                        records
                    )
                    conn.commit()

                if DATABASE_TYPE == 2:
                    collection.bulk_write(records)

                print(f"Inserted {message_counter} records...")
                records.clear()  # Clear the list for the next batch
                topics.clear()

        if records:
            if DATABASE_TYPE == 1:
                execute_values(
                    cursor,
                    "INSERT INTO retained (topic, levels) VALUES %s",
                    records
                )
                conn.commit()

            if DATABASE_TYPE == 2:
                collection.bulk_write(records)

            print(f"Inserted remaining {len(records)} records...")

        print("Data insertion complete.")

    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    create_test_data()
