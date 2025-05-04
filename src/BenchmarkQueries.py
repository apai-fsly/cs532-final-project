import time
import mysql.connector
from mysql.connector import Error
import statistics
import matplotlib.pyplot as plt

def run_query(connection_config, query, iterations=3):
    """Run a query multiple times and return average performance metrics"""
    times = []
    result_count = 0
    
    try:
        connection = mysql.connector.connect(**connection_config)
        cursor = connection.cursor()

        for _ in range(iterations):
            # Clear cache between runs for more accurate comparison
            cursor.execute("RESET QUERY CACHE;") if "RESET QUERY CACHE" in dir(cursor) else None
            
            start_time = time.time()
            cursor.execute(query)
            result = cursor.fetchall()
            elapsed_time = time.time() - start_time
            
            times.append(elapsed_time)
            result_count = len(result)

        cursor.close()
        connection.close()

        avg_time = statistics.mean(times)
        print(f"[{connection_config['storage_type']}] {query.strip()[:50]}... took avg {avg_time:.4f}s (±{statistics.stdev(times):.4f}) over {iterations} runs, returned {result_count} rows")
        return avg_time

    except Error as e:
        print(f"Error: {e}")
        return None

def plot_results(results):
    """Visualize the performance comparison"""
    queries = list(results['SSD'].keys())
    ssd_times = list(results['SSD'].values())
    hdd_times = list(results['HDD'].values())

    x = range(len(queries))
    width = 0.35

    fig, ax = plt.subplots()
    ssd_bars = ax.bar([i - width/2 for i in x], ssd_times, width, label='SSD', color='orange')
    hdd_bars = ax.bar([i + width/2 for i in x], hdd_times, width, label='HDD', color='blue')

    ax.set_ylabel('Execution Time (seconds)')
    ax.set_title('HDD vs SSD Query Performance Comparison')
    ax.set_xticks(x)
    ax.set_xticklabels([q[:20] + '...' for q in queries], rotation=45, ha='right')
    ax.legend()

    fig.tight_layout()
    plt.savefig('storage_performance_comparison.png')
    plt.show()

def main():
    # Define your test queries
    query_list = [
        "SELECT COUNT(*) FROM TitleBasics;",
        "SELECT * FROM TitleRatings WHERE averageRating > 8;",
        "SELECT t.primaryTitle, r.averageRating FROM TitleBasics t JOIN TitleRatings r ON t.tconst = r.tconst WHERE r.numVotes > 10000;",
        "SELECT primaryTitle, startYear FROM TitleBasics WHERE startYear BETWEEN 2000 AND 2010 ORDER BY startYear DESC;",
        "SELECT genres, COUNT(*) as count FROM TitleBasics GROUP BY genres ORDER BY count DESC LIMIT 10;" 
    ]

    # Configuration - modify these with your actual connection details
    configs = {
        "SSD": {
            'host': '127.0.0.1',
            'port': 3306,
            'user': 'myuser',
            'password': 'mypassword',
            'database': 'imdb_ssd',
            'storage_type': 'SSD'
        },
        "HDD": {
            'host': '127.0.0.1',
            'port': 3307,  # Different port for HDD instance
            'user': 'myuser',
            'password': 'mypassword',
            'database': 'imdb_hdd',
            'storage_type': 'HDD'
        }
    }

    results = {'SSD': {}, 'HDD': {}}

    for query in query_list:
        for label, config in configs.items():
            print(f"\n--- Testing on {label} storage ---")
            time_taken = run_query(config, query)
            if time_taken is not None:
                results[label][query] = time_taken
            else:
                print(f"Query failed on {label} storage")
    
    # Generate visualization
    plot_results(results)

    # Print summary table
    print("\nPerformance Summary:")
    print(f"{'Query':<60} {'SSD (s)':<10} {'HDD (s)':<10} {'Speedup':<10}")
    for query in query_list:
        ssd_time = results['SSD'].get(query, 0)
        hdd_time = results['HDD'].get(query, 0)
        speedup = hdd_time / ssd_time if ssd_time > 0 else 0
        print(f"{query[:60]:<60} {ssd_time:<10.4f} {hdd_time:<10.4f} {speedup:<10.1f}x")

if __name__ == "__main__":
    main()