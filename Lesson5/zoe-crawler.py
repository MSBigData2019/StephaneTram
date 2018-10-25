import requests
from bs4 import BeautifulSoup
import re




url = 'https://www.paruvendu.fr/auto-moto/listefo/default/default?r2=VVORE000+&md=VVOREZOE&lo='
urlPrefix = 'https://www.lacentrale.fr'

regions = {'Nouvelle Aquitaine' : ['16','17','19','23','24','33','40','47','64','79','86','87'], 'PACA' : ['04','05','06','13','83','84'], 'Ile de France' : ['75','77','78','91','92','93','94','95']}

for reg in regions:
    print(reg)
    soup = BeautifulSoup(requests.get(url + '%2C'.join(regions[reg])+'%2C'.text,'html.parser')
    numAnn = soup.find('span', {'class':'numAnn'}).string
    links = soup.find_all('a',{'class' : 'linkAd annJB'})
    for link in links:
        reSoup = BeautifulSoup(requests.get(urlPrefix + link.attrs['href']).text,'html.parser')
        modele = reSoup.find_all('div', {'class' : 'versionTxt txtGrey7C sizeC mB10 hiddenPhone'})[0].string
        prix = reSoup.find('div', {'class' : 'gpfzj'}).contents[1].string.string.strip()
        

        annee = reSoup.find('span', {'class' : 'kmTxt sizeC'}).contents[1].string.strip()
        km = reSoup.find('span', {'class' : 'kmTxt sizeC'}).contents[5].string.strip()

        sellerType = reSoup.find_all('div', {'class' : 'bold italic mB10'})[0].string
        dpt = reSoup.find_all('h3', {'class':'mb10 noBold'})

        print(modele)
        print(prix)
        print(annee)
        print(km)
        print(sellerType)
        print(dpt)




