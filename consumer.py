from kafka import KafkaConsumer
import psycopg2
import json

consumer = KafkaConsumer(
    'cricket',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='cricket-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

conn = psycopg2.connect(
    "postgresql://esports_db_j0cg_user:esports%40123@dpg-d76eihuuk2gs73etq9k0-a.oregon-postgres.render.com:5432/esports_db_j0cg"
)

cur = conn.cursor()

print("Consumer started...")

for message in consumer:
    data = message.value
    print("Received:", data)

    try:
        cur.execute(
            """
            INSERT INTO match_stats 
            (match_id, player, team, runs, balls, wickets, fours, sixes, event_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                data["match_id"],
                data["player"],
                data["team"],
                data["runs"],
                data["balls"],
                data["wickets"],
                data["fours"],
                data["sixes"],
                data["event_time"] # Use event_time from the Producer
            )
        )

        conn.commit()
        print(f"Recorded: {data['player']} scored {data['runs']} runs")

    except Exception as e:
        print("Error inserting data:", e)
        conn.rollback()