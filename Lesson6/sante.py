import pandas as pd
import matplotlib.pyplot as plt

staff = pd.read_csv('./doc/PERINAT_P_2017.csv',sep=';')
hosp = pd.read_csv('./doc/ID_2017.csv',sep=';',encoding='latin-1')
mat = pd.read_csv('./doc/PERINAT_2017.csv',sep=';')
 
sf = staff.pivot(index='FI',columns='PERSO',values='ETP').fillna(0)
ho = hosp[['fi','dep_diff','COMINSEE','NOMCOM']].rename(columns={'fi':'FI'})
ma = mat[['FI','AUTOR','LIT_OBS','SALTRAV','ENFANTS','ENFTR']].fillna(0)
 
df = ho.set_index('FI').join(sf).join(ma.set_index('FI')).dropna(subset=['ENFANTS'])
df = df[df.ENFANTS!=0]
df = df[df.AUTOR!='CPPR']
agg = df.groupby(['dep_diff']).sum()

fig = plt.figure()
plt.plot(agg.ENFANTS,agg.ENFTR,'o')
plt.show()

fig2 = plt.figure()
plt.plot(df.SALTRAV,df.M2050,'o')
plt.show()
