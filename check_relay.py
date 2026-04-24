import urllib.request
import json

data = json.loads(urllib.request.urlopen('http://localhost:8000/angel/search-mcx?type=futures').read())
gold = [i for i in data if i['name'] == 'GOLD']
for g in gold:
    print(g)
