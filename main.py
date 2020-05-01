import time
import reader_getter
import reader_scraper


def main():
    filename = input('Enter the name of your .csv (ex. filename.csv): ')
    mode = input('Scraper or API? [S/A]: ')
    if mode=='S':
        start_time = time.time()
        reader_scraper.scrape_from_bigquery_csv(filename)
    elif mode=='A':
        key = input('Enter your API key: ')
        start_time = time.time()
        reader_getter.get_source_code_from_bigquery_csv(filename, key)
    else:
        print('Select a valid response [S/A].')
        return
    print("\nEXECUTION TIME --- %s seconds ---\n" % (time.time() - start_time))
    print('\n\nDone.')


main()
