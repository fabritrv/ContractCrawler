import requests
import os
import csv
from concurrent.futures import ThreadPoolExecutor

_addr1=''
_addr2=''
_addr3=''
_addr4=''
_addr5=''
_key="8D2C78JB9VS574RVEUHVTJY3WHHDAJEENX" #API key
_p1="https://api.etherscan.io/api?module=contract&action=getsourcecode&address="
_keyparam="&apikey="
_threader=0
_fold=os.getcwd()+'\\reader_getter_data'

def get_source_code_from_bigquery_csv(filename):
    global _threader
    global _fold
    line_count = 0
    if not os.path.isdir(_fold):
        os.makedirs(_fold)
    with open(filename) as csv_file:
        csvr = csv.reader(csv_file)
        for row in csvr:
            if line_count == 0: #the first line is a note
                print(f'\nStarting to read {filename}')
                _threader+=1
            else:
                filename=row[0]+".sol"
                loc=_fold+'\\'+filename
                if os.path.isfile(loc):
                    continue
                else:
                    url_handler(_threader,row[0])
                    if _threader==6:
                        api_threader()
            line_count += 1
            print("\rContracts analyzed - {:.8f}%".format((line_count / 20467002) * 100), end=" ")
    print(f'Processed {line_count-1} contracts.')

def url_handler(i,address):
    global _threader
    global _addr1
    global _addr2
    global _addr3
    global _addr4
    global _addr5
    if i==1:
        _addr1=address
        _threader+=1
        return
    elif i==2:
        _addr2=address
        _threader+=1
        return
    elif i==3:
        _addr3=address
        _threader+=1
        return
    elif i==4:
        _addr4=address
        _threader+=1
        return
    elif i==5:
        _addr5=address
        _threader+=1
    else:
        _threader=1
        url_handler(_threader,address)

def api_threader():
    global _addr1
    global _addr2
    global _addr3
    global _addr4
    global _addr5
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.submit(api_call, _addr1)
        executor.submit(api_call, _addr2)
        executor.submit(api_call, _addr3)
        executor.submit(api_call, _addr4)
        executor.submit(api_call, _addr5)

def api_call(addr):
    global _p1
    global _key
    global _keyparam
    global _fold
    url=_p1+addr+_keyparam+_key
    response = requests.get(url)
    source_code = response.json()['result'][0]['SourceCode']
    if source_code!='':
        filename=addr+".sol"
        loc=_fold+'\\'+filename
        f = open(loc, "w", encoding = "utf-8")
        f.write(source_code)
        f.close