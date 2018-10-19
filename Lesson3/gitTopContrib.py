from bs4 import BeautifulSoup
import requests
import json
import sys

def getSoupFromUrl(url):
    res=requests.get(url)
    html_doc = res.text.replace('&nbsp;',' ').replace('\t','').replace('\r','').replace('\n','')
    soup = BeautifulSoup(html_doc,'html.parser')
    return soup

def getUserNames(soup):
    article = soup.find_all('article', {'class':'markdown-body entry-content'})
    cells = [x.find_all('td') for x in article]
    links = [y.find_all('a') for x in cells for y in x]
    flatLinks = [item for sublist in links for item in sublist]
    return [x.string for x in list(filter(lambda x: not(x.has_attr('rel')),flatLinks))]
    
def addUserStarCount(userNames):
    userList={}
    for userName in userNames:
        nbRepos = int(requests.get('https://api.github.com/users/'+userName, headers = header).json()['public_repos']) 
        for i in list(range(max(1,int(nbRepos/100)))):
            params ={'page':i+1,'per_page':100}
            res = requests.get('http://api.github.com/users/'+userName+'/repos', headers = header,params=params)
            d = res.json()
            for x in d:
                if userName in userList: 
                    userList[userName].append(x['stargazers_count'])
                else:
                    userList[userName] = [x['stargazers_count']]
    return userList

tokenString = sys.argv[1]
print(tokenString)
header = {'Authorization':'token ' + tokenString}
soup = getSoupFromUrl('https://gist.github.com/paulmillr/2657075')
nameList = getUserNames(soup)
userList = addUserStarCount(nameList)

#print(sorted(userList,key = lambda x: x[1], reverse=True))

print(sorted([(i,sum(userList[i])/len(userList[i])) for i in userList.keys()], key=lambda x : x[1], reverse=True))

