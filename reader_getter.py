import requests
import os
import csv
from concurrent.futures import ThreadPoolExecutor

_key="" #API key
_p1="https://api.etherscan.io/api?module=contract&action=getsourcecode&address="
_keyparam="&apikey="
_fold=os.getcwd()+'\\reader_getter_data'

def get_source_code_from_bigquery_csv(filename):
    global _fold
    address_list=[]
    line_count = 0
    if not os.path.isdir(_fold):
        os.makedirs(_fold)
    with open(filename) as csv_file:
        csvr = csv.reader(csv_file)
        for row in csvr:
            if line_count == 0: #the first line is a note
                print(f'\nStarting to read {filename}')
            else:
                filename=row[0]+".txt"
                loc=_fold+'\\'+filename
                if os.path.isfile(loc):
                    continue
                else:
                    address_list.append(row[0])
                    if len(address_list)==5:
                        api_threader(address_list)
                        address_list=[]
            line_count += 1
            print("\rContracts analyzed - {:.8f}%".format((line_count / 20467002) * 100), end=" ")
    print(f'Processed {line_count-1} contracts.')

def api_threader(address_list):
    with ThreadPoolExecutor(max_workers=5) as executor:
        for a in address_list:
            executor.submit(api_call, a)

def api_call(addr):
    global _p1
    global _key
    global _keyparam
    global _fold
    url=_p1+addr+_keyparam+_key
    response = requests.get(url)
    source_code = response.json()['result'][0]['SourceCode']
    if source_code!='':
        filename=addr+".txt"
        loc=_fold+'\\'+filename
        f = open(loc, "w", encoding = "utf-8")
        f.write(source_code)
        f.close
