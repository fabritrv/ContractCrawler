import time
import reader_getter
import reader_scraper

def main():
    start_time = time.time()
    #threaded scraper, 100 parallel calls, no source code in multiple files
    #reader_scraper.scrape_from_bigquery_csv('bigquery_2018.csv') 
    #threaded API caller, 5 calls/sec
    reader_getter.get_source_code_from_bigquery_csv('bigquery_2018.csv')
    print("\nEXECUTION TIME --- %s seconds ---\n" % (time.time() - start_time))
    print('\n\nDone.')

main()
