import csv
import ntpath
from zipfile import ZipFile

import requests
from mechanize import Browser
from tqdm import tqdm

url = "https://www.bseindia.com/markets/MarketInfo/BhavCopy.aspx"
br = Browser()
br.set_handle_robots(False)
br.open(url)

main_url = None

for link in br.links():
    # print link.url

    if (link.url.startswith('http://www.bseindia.com/download/BhavCopy/Equity/') or link.url.startswith(
            'https://www.bseindia.com/download/BhavCopy/Equity/')) and (
            link.url.endswith('_CSV.ZIP') or link.url.endswith('csv.zip')):
        main_url = link.url

filename = ntpath.basename(main_url)
response = requests.get(main_url, stream=True)

with open(filename, "wb") as handle:
    for data in tqdm(response.iter_content()):
        handle.write(data)

csv_filename = None

with ZipFile(filename, 'r') as zip:
    # printing all the contents of the zip file
    zip.printdir()

    csv_filename = zip.NameToInfo.keys()[0]

    # extracting all the files
    print('Extracting all the files now...')
    zip.extractall()
    print('Done!')

index_of_SC_CODE = 0
index_of_SC_NAME = 1
index_of_OPEN = 4
index_of_HIGH = 5
index_of_LOW = 6
index_of_CLOSE = 7

out_file = open('parsed_data.csv', 'a')
out_file.truncate(0)
header = 'SC_CODE,SC_NAME,OPEN,HIGH,LOW,CLOSE\n'
out_file.write(header)

with open(csv_filename, 'r') as csvFile:
    reader = csv.reader(csvFile)

    reader = iter(reader)
    next(reader)

    for row in reader:
        out_file.write('%s,%s,%s,%s,%s,%s\n' % (
            row[index_of_SC_CODE], row[index_of_SC_NAME], row[index_of_OPEN], row[index_of_HIGH],
            row[index_of_LOW], row[index_of_CLOSE]))

csvFile.close()
out_file.close()
