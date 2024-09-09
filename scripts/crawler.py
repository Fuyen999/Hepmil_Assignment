from dotenv import dotenv_values
import os
import psycopg2
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import praw

TOP_N_MEME = 20

dotenv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
config = dotenv_values(dotenv_path)
HOSTNAME = config['HOSTNAME']
DATABASE = config['DATABASE']
USERNAME = config['USERNAME']
PASSWORD = config['PASSWORD']
PORT_ID = config['PORT_ID']
CLIENT_ID = config['REDDIT_CLIENT_ID']
CLIENT_SECRET = config['REDDIT_SECRET']
REDDIT_USERNAME = config['REDDIT_USERNAME']
REDDIT_PASSWORD = config['REDDIT_PASSWORD']

def connect_database():
    cur = conn = None

    try:
        # Connect to database
        conn = psycopg2.connect(
            host = HOSTNAME,
            dbname = DATABASE,
            user = USERNAME,
            password = PASSWORD,
            port = PORT_ID
        )

        # Cur stores return object from queries
        cur = conn.cursor()

        print("Database connected")
        return cur, conn

    except Exception as error:
        print(error)
        return None, None


def sqlalchemy_connect():
    uri = f"postgresql+psycopg2://{quote_plus(USERNAME)}:{quote_plus(PASSWORD)}@{HOSTNAME}:{PORT_ID}/{DATABASE}"
    alchemyEngine = create_engine(uri)
    return alchemyEngine


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


def get_top_memes():
    reddit = praw.Reddit(
        client_id = CLIENT_ID,
        client_secret = CLIENT_SECRET,
        user_agent = "hepmil by u/ssamu_iz",
        username = REDDIT_USERNAME,
        password = REDDIT_PASSWORD
    )

    subreddit = reddit.subreddit("memes")
    top_posts = subreddit.top(time_filter="day", limit=TOP_N_MEME)
    memes_full_data = []
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fields = ("name", "title", "author_fullname", "url", "thumbnail", "ups", "downs")
    for post in top_posts:
        to_dict = vars(post)
        sub_dict = {field:to_dict[field] for field in fields}
        memes_full_data.append(sub_dict)
    
    return memes_full_data, timestamp


    # url = "https://reddit.com/r/memes/top.json"
    # params = {
    #     "t": "day",
    #     "limit": TOP_N_MEME
    # }
    # headers = get_access_token()

    # try:
    #     print("Fetching data from reddit ...")
    #     async with aiohttp.ClientSession(headers=headers) as session:
    #         async with session.get(url, params=params) as response:
    #             data = await response.json()
    #             timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    #             return data, timestamp
            
    # except Exception as error:
    #     print(error)
    #     return None, None


def newest_update():
    # response, timestamp = get_top_memes()
    # memes_full_data = response["data"]["children"]

    memes_full_data, timestamp = get_top_memes()

    # memes_data_list = [[
    #     meme["data"]["name"],
    #     meme["data"]["title"],
    #     meme["data"]["author_fullname"],
    #     meme["data"]["url"],
    #     meme["data"]["thumbnail"]
    # ] for meme in memes_full_data]

    # votes_data_list = [[
    #     meme["data"]["name"],
    #     meme["data"]["ups"],
    #     meme["data"]["downs"],
    #     timestamp
    # ] for meme in memes_full_data]

    memes_data_list = [[
        meme["name"],
        meme["title"],
        meme["author_fullname"],
        meme["url"],
        meme["thumbnail"]
    ] for meme in memes_full_data]

    votes_data_list = [[
        meme["name"],
        meme["ups"],
        meme["downs"],
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
    newest_update()

