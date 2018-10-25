import pandas as pd
import requests

req = requests.get('https://www.open-medicaments.fr/api/v1/medicaments?query=paracetamol')
medList = req.json()

res = {}

for med in medList:
    res2 = requests.get('https://www.open-medicaments.fr/api/v1/medicaments/'+str(med['codeCIS']))
    medProp = res2.json()
    denom = medProp['denomination']
    forme = medProp['formePharmaceutique']
    dosage = medProp['compositions'][0]['substancesActives'][0]['dosageSubstance']




