import psycopg2  # type: ignore
from confluent_kafka import Consumer, KafkaException, KafkaError, SerializingProducer  # type: ignore
import simplejson as json  # type: ignore
import random
from datetime import datetime
import time

conf = {
    'bootstrap.servers': 'localhost:9092'
}

consumer = Consumer(conf | {
    'group.id': 'voting-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False 
})

producer = SerializingProducer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"delivery failed: {err}")
    else:
        print(f"message delivered to: {msg.topic()} [{msg.partition()}]")

if __name__ == "__main__":
    conn = psycopg2.connect("host = localhost dbname = voting user = postgres password = postgres")
    cur = conn.cursor()

    cadidates_query = cur.execute(
        """
        SELECT row_to_json(col)
        FROM (
            SELECT * FROM CANDIDATES
            ) col;
        """
    )
    candidates = cur.fetchall()
    candidates = [candidate[0] for candidate in candidates]
    
    if len(candidates) == 0:
        raise Exception("No candidates in database")
    else:
        print(candidates)

    consumer.subscribe(['voters_topic'])

    try:
        while True:
            msg = consumer.poll(timeout = 1.0)
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                voter = json.loads(msg.value().decode('utf-8'))
                chosen_candidate = random.choice(candidates)
                vote = voter | chosen_candidate | {
                    'voting_time': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    'vote': 1
                }

                try:
                    print('User {} is voting for candidate: {}'.format(vote['voter_id'], vote['candidate_id']))
                    cur.execute(
                        """
                        INSERT INTO VOTES (VOTER_ID, CANDIDATE_ID, VOTING_TIME)
                        VALUES (%s, %s, %s)
                        """, (vote['voter_id'], vote['candidate_id'], vote['voting_time'])
                    )
                    conn.commit()
                    producer.produce(
                        topic = 'votes_topic',
                        key = vote['voter_id'],
                        value = json.dumps(vote),
                        on_delivery = delivery_report
                    )
                    producer.poll(0)

                except Exception as e:
                    print('Error ', e)
            time.sleep(0.5)
    except Exception as e:
        print(e)