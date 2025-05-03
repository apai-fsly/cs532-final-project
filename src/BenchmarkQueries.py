import time
import mysql.connector
from mysql.connector import Error


def run_query(connection_config, query):
    try:
        connection = mysql.connector.connect(**connection_config)
        cursor = connection.cursor()

        start_time = time.time()
        cursor.execute(query)
        result = cursor.fetchall()
        elapsed_time = time.time() - start_time

        print(f"[{connection_config['host']}] {query.strip()} took {elapsed_time:.4f} seconds and returned {len(result)} rows.")

        cursor.close()
        connection.close()

        return elapsed_time

    except Error as e:
        print(f"Error: {e}")
        return None


def main():
    query_list = [
        "SELECT COUNT(*) FROM TitleBasics;",
        "SELECT * FROM TitleRatings WHERE movieRating > 8;",
        "SELECT t.primaryTitle, r.movieRating FROM TitleBasics t JOIN TitleRatings r ON t.tconst = r.tconst WHERE r.numVotes > 10000;"
    ]

    configs = {
        "SSD": {
            'host': '127.0.0.1',
            'port': 3306,  # mysql_ssd
            'user': 'myuser',
            'password': 'mypassword',
            'database': 'mydb'
        },
        "HDD": {
            'host': '127.0.0.1',
            'port': 3306,  # mysql_hdd
            'user': 'myuser',
            'password': 'mypassword',
            'database': 'mydb'
        }
    }

    for query in query_list:
        for label, config in configs.items():
            print(f"\nRunning on {label} (host: {config['host']}, port: {config['port']})")
            time_taken = run_query(config, query)
            if time_taken is not None:
                print(f"{label} query took: {time_taken:.4f} seconds")
            else:
                print(f"{label} query failed.")


if __name__ == "__main__":
    main()
