import psycopg2
import requests
import random
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = 'https://randomuser.me/api/?nat=gb'
PARTIES = ['Labour Party', 'Social Democratic Party', 'Liberal Party']

random.seed(4)

def create_tables(conn, cur):
    cur.execute(
        """
            CREATE TABLE IF NOT EXISTS candidates(
            candidate_id varchar(255) primary key,
            candidate_name varchar(255),
            party_affiliation varchar(255),
            biography text,
            campaign_platform text,
            photo_url text
            )
        """
    )

    cur.execute(
        """
            CREATE TABLE IF NOT EXISTS voters(
            voter_id varchar(255) primary key,
            voter_name varchar(255),
            date_of_birth varchar(255),
            gender varchar(255),
            nationality varchar(255),
            address_street varchar(255),
            address_city varchar(255),
            address_state varchar(255),
            address_country varchar(255),
            address_postcode varchar(255),
            email varchar(255),
            phone_number varchar(255),
            picture text,
            registed_age integer 
            )
        """
    )

    cur.execute(
        """
            CREATE TABLE IF NOT EXISTS votes(
            voter_id varchar(255) unique,
            candidate_id varchar(255),
            voting_time varchar(255),
            vote int default 1,
            primary key (voter_id, candidate_id)
            )
        """
    )

    conn.commit()


def generate_candidate_data(candidate_number, total_parties):

    session = requests.Session()

    retry_strat = Retry(
         total = 5,
         backoff_factor = 1,
         status_forcelist = [429, 500, 502, 503, 504],
         method_whitelist = ['HEAD', 'GET', 'OPTIONS']
    )

    adapter = HTTPAdapter(max_retries=retry_strat)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    
    response = session.get(BASE_URL + '&gender=' + ('female' if candidate_number % 2 == 1 else 'male'))
    if response.status_code == 200:
            user_data = response.json()['results'][0]

            return {
                 'candidate_id': user_data['login']['uuid'],
                 'candidate_name': f"{user_data['name']['first']} {user_data['name']['last']}",
                 'party_affiliation': PARTIES[candidate_number % total_parties],
                 'biography': 'Bio of the candidate',
                 'campaign_platform': 'Campaign agenda and platform',
                 'photo_url': user_data['picture']['large']
            }
    else:
         return "error fetching candidate data"

if __name__ == "__main__":
    try:
        conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
        cur = conn.cursor()

        create_tables(conn, cur)

        cur.execute(
            """
                select * from candidates;
            """
        )

        candidates = cur.fetchall()
        print(candidates)

        if len(candidates) == 0:
            for i in range(3):
                candidate = generate_candidate_data(i, 3)
                print(candidate)
                cur.execute("""
                        INSERT INTO candidates (candidate_id, candidate_name, party_affiliation, biography, campaign_platform, photo_url)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                candidate['candidate_id'], candidate['candidate_name'], candidate['party_affiliation'], candidate['biography'],
                candidate['campaign_platform'], candidate['photo_url']))
                conn.commit()

    except Exception as e:
        print(e)
