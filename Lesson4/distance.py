



import requests

params={'api':1,'origin':'Paris','destination':'Marseille'}
res = requests.get('https://maps.googleapis.com/maps/api/directions/json?',params=params)

