import aiohttp
import asyncio
from dotenv import load_dotenv
import os
import psycopg2
from datetime import datetime, timedelta

TOP_N_MEME = 20

def connect_database():
    cur = conn = None

    try:
        # Load database related env variables
        load_dotenv(override=True)

        # Connect to database
        conn = psycopg2.connect(
            host = os.getenv('HOSTNAME'),
            dbname = os.getenv('DATABASE'),
            user = os.getenv('USERNAME'),
            password = os.getenv('PASSWORD'),
            port = os.getenv('PORT_ID')
        )

        # Cur stores return object from queries
        cur = conn.cursor()

        print("Database connected")
        return cur, conn

    except Exception as error:
        print(error)
        return None, None


def close_database_connection(conn, cur):
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()
    print("Database connection closed")


def init_database(cur, conn):
    try:
        if cur is None or conn is None:
            cur, conn = connect_database()

        memes_create_script = ''' CREATE TABLE IF NOT EXISTS memes (
                                name            VARCHAR(20) PRIMARY KEY,
                                title           TEXT,
                                author          VARCHAR(20),
                                url             TEXT,
                                thumbnail_url   TEXT
                            )'''

        upvotes_create_script = ''' CREATE TABLE IF NOT EXISTS votes (
                                name            VARCHAR(20),
                                upvotes         INT,
                                downvotes       INT,
                                crawled_at      TIMESTAMP
                            )'''
        
        print("Creating memes table")
        cur.execute(memes_create_script)
        print("Creating votes table")
        cur.execute(upvotes_create_script)
        conn.commit()

        return cur, conn

    except Exception as error:
        print(error)
        close_database_connection(cur, conn)
        return None, None
    

def insert_data(cur, conn, table, data_list, no_of_rows=TOP_N_MEME, ignore_conflict=True):
    try:
        # Check if database connected
        if cur is None or conn is None:
            cur, conn = connect_database()
        
        # Check if table exists
        cur.execute(f"""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{table}';
        """)
        if cur.fetchone()[0] != 1:
            cur, conn = init_database(cur, conn)

        # INSERT SQL statement (prepared statement)
        insert_script = f'''INSERT INTO {table} 
                            VALUES {", ".join([f"({", ".join(["%s" for i in range(len(data_list[0]))])})" for i in range(no_of_rows)])}
                            {"ON CONFLICT (name) DO NOTHING" if ignore_conflict else ""}'''
        
        # Exceute statement
        print(f"Inserting data into {table} ...")
        cur.execute(insert_script, tuple([value for data in data_list for value in data]))
        
        return cur, conn
    
    except Exception as error:
        print(error)
        close_database_connection(cur, conn)
        return None, None


def delete_outdated_data(cur, conn):
    # Delete data from 24 hours ago
    try:
        yesterday = (datetime.now() - timedelta(days = 1)).strftime("%Y-%m-%d %H:%M:%S")
        delete_script = f'''DELETE FROM votes
                            WHERE crawled_at < '{yesterday}\''''
        cur.execute(delete_script)
        print("Deleted outdated data")
        return cur, conn
    
    except Exception as error:
        print(error)
        close_database_connection(cur, conn)
        return None, None


async def get_top_memes():
    url = "https://reddit.com/r/memes/top.json"
    params = {
        "t": "day",
        "limit": TOP_N_MEME
    }

    try:
        print("Fetching data from reddit ...")
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                data = await response.json()
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                return data, timestamp
            
    except Exception as error:
        print(error)
        return None, None


async def main():
    response, timestamp = await get_top_memes()
    memes_full_data = response["data"]["children"]

    memes_data_list = [[
        meme["data"]["name"],
        meme["data"]["title"],
        meme["data"]["author_fullname"],
        meme["data"]["url"],
        meme["data"]["thumbnail"]
    ] for meme in memes_full_data]

    votes_data_list = [[
        meme["data"]["name"],
        meme["data"]["ups"],
        meme["data"]["downs"],
        timestamp
    ] for meme in memes_full_data]

    cur, conn = connect_database()
    cur, conn = insert_data(cur, conn, "memes", memes_data_list)
    cur, conn = insert_data(cur, conn, "votes", votes_data_list, ignore_conflict=False)
    cur, conn = delete_outdated_data(cur, conn)
    conn.commit()

    close_database_connection(cur, conn)


if __name__ == '__main__':
    print("Running crawler ...")
    asyncio.run(main())

