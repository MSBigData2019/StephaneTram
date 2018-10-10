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

stock_name = sys.argv[1]
url='https://www.reuters.com/finance/stocks/financial-highlights/'
headerData = parseModules(getModulesFromHeader(getSoupFromUrl(url,stock_name)))
dataDict = {**parseModules(getModulesFromHeader(getSoupFromUrl(url,stock_name))), **parseModules(getModulesFromColumns(getSoupFromUrl(url,stock_name)))}

print(stock_name+' Sales in Q4 18 : ')
print(dataDict['Quarter Ending Dec-18'])
print(stock_name+' Stock price : ')
print(dataDict['spot price'])
print(stock_name+' Stock change : ')
print(dataDict['spot change'])
print(stock_name+' % Shares Owned : ')
print(dataDict['% Shares Owned:'])
print(stock_name+' Div Yield : ')
print(dataDict['Dividend Yield'])

