import sqlite3
from sqlite3 import Error
import os
import csv

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        if os.path.exists(db_file):
            os.remove(db_file)
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
       
def remove_symbol(name :str) -> str:
    '''
    Remove symbol annotations from batter name field '\xa0(c)†', '\xa0(c)', '\xa0†'
    '''
    return name.split('\xa0')[0]

def get_is_out_status(status_note :str) -> bool:
    return False if 'not out' in status_note  else True
        
def load_batter_score(con, dir :str) -> None:
    cur = con.cursor()
    for fname in os.listdir(dir):
        fpath = os.path.join(dir, fname)
        with open(fpath,'r') as file:
            match_metadata = fname.split('_')
            team_name = match_metadata[-1][:-4]
            match_id = int(match_metadata[0])
            game_id = int(match_metadata[1])
            match_title = match_metadata[-2]
            league_year = dir.split('/')[-1]
            dr = csv.DictReader(file)
            
            to_db = [(remove_symbol(i['Batter']), i['IsOut'], get_is_out_status(i['IsOut']), i['Runs'], i['Balls'], i['4s'], i['6s'], team_name, match_id, game_id, match_title, league_year) for i in dr]

            cur.executemany("   INSERT INTO scores (BatterName, IsOutNote, IsOut, RunCount, BallCount, Fours, Sixes, TeamName, MatchId, GameId, MatchTitle, LeagueYear) \
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", to_db)
    con.commit()

    

def main():
    database = 'sql/cricket.db'

    conn = create_connection(database)
    
    if conn is not None:
        with open('sql/make_scores_table.sql', 'r') as sql_file:
            sql_create_scores_table = sql_file.read()
        
        create_table(conn, sql_create_scores_table)
        
        load_batter_score(conn, 'data/2023')
        load_batter_score(conn, 'data/2022')
        
        conn.close()
    else:
        print("Error! cannot create the database connection.")

if __name__ == '__main__':
    main()