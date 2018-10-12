import requests
from bs4 import BeautifulSoup
from bs4 import NavigableString
import sys

def getSoupFromUrl(url,stock_name):
    res = requests.get(url+stock_name)
    html_doc = res.text.replace('&nbsp;',' ').replace("\t", "").replace("\r", "").replace("\n", "")
    soup = BeautifulSoup(html_doc,'html.parser')
    return soup


def getModulesFromColumns(soup):
    columns = soup.find_all('div',{'class':'sectionColumns'})
    modules = [item for sublist in [x.find_all('div', class_='module') for x in columns] for item in sublist]
    return modules

def getModulesFromHeader(soup):
    siteHeader = soup.find_all('div', {'id':'sectionHeader'})
    modules = [item for sublist in [x.find_all('div', class_='module') for x in siteHeader] for item in sublist]
    return modules

def parseModules(modules):
    dataTable = {}
    for i in range(len(modules)):
        ## Traitement des cadres dans le bandeau recap
        details = modules[i].find_all('div',{'class':'sectionQuote nasdaqChange'})
        if len(details)>0:
            rowData = {}
            rowData[details[0].find_all('span')[0].string] = details[0].find_all('span')[1].string
            dataTable['spot price'] = rowData
           
        details = modules[i].find_all('div',{'class':'sectionQuote priceChange'})
        if len(details)>0:
            rowData = {}
            rowData[details[0].find_all('span')[0].string.strip()] = details[0].find_all('span')[4].string.strip()
            dataTable['spot change'] = rowData

        ## Traitement des tableaux dans le corps de la page
        header = [x.string for x in modules[i].find_all('th',{'class':'data'})]
        rows = modules[i].find_all('tr')
        for x in rows:
            if not isinstance(x.contents[0],NavigableString):
                if x.contents[0].has_attr('class'):
                    if x.contents[0].attrs['class'][0] == 'dataTitle':
                        dataType = x.contents[0].string.strip()

                if x.has_attr('class') == True:
                    rowData = {}
                    tX = x.find_all('td')
                    for y in range(len(x)):
                        if not (tX[y].has_attr('class')):
                            dataLineName = tX[y].string
                        if tX[y].has_attr('class'):
                            if tX[y].attrs['class'][0] == 'data':
                                rowData[dataLineName if len(header) == 0 else header[y-1]]=tX[y].string
                    dataTable[dataLineName]=rowData
    return dataTable

def launch_requests(stock_name):
    url='https://www.reuters.com/finance/stocks/financial-highlights/'
    soup = getSoupFromUrl(url,stock_name)
    dataDict = {**parseModules(getModulesFromHeader(soup)), **parseModules(getModulesFromColumns(soup))}
    return dataDict

if len(sys.argv) <2:
    stocks = ['LVMH.PA','DANO.PA','AIR.PA']
else:
    stocks=sys.argv[1].split(',')

for x in stocks:
    dataDict = launch_requests(x)
    print(x+' Sales in Q4 18 : ')
    print(dataDict['Quarter Ending Dec-18'])
    print(x+' Stock price : ')
    print(dataDict['spot price'])
    print(x+' Stock change : ')
    print(dataDict['spot change'])
    print(x+' % Shares Owned : ')
    print(dataDict['% Shares Owned:'])
    print(x+' Div Yield : ')
    print(dataDict['Dividend Yield'])

