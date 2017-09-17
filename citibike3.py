#!/usr/bin/env python
# coding: utf-8


import geopy
import geopy.distance
import pandas as pd
import pytz
import pymongo
from pymongo import MongoClient
from datetime import datetime
from datetime import timedelta
import time
##########################################################
#Liste des fréquences de passage :
passage_metro_prevu=[{'ligne':1,'interval':"A",'freq':20},
         {'ligne':1,'interval':"B",'freq':8},
         {'ligne':1,'interval':"C",'freq':6},
         {'ligne':1,'interval':"D",'freq':10},
         {'ligne':1,'interval':"E",'freq':3},
         {'ligne':2,'interval':"A",'freq':20},
         {'ligne':2,'interval':"B",'freq':8},
         {'ligne':2,'interval':"C",'freq':7},
         {'ligne':2,'interval':"D",'freq':8},
         {'ligne':2,'interval':"E",'freq':6},
         {'ligne':3,'interval':"A",'freq':20},
         {'ligne':3,'interval':"B",'freq':12},
         {'ligne':3,'interval':"C",'freq':8},
         {'ligne':3,'interval':"D",'freq':12},
         {'ligne':3,'interval':"E",'freq':6},
         {'ligne':4,'interval':"A",'freq':20},
         {'ligne':4,'interval':"B",'freq':8},
         {'ligne':4,'interval':"C",'freq':7},
         {'ligne':4,'interval':"D",'freq':10},
         {'ligne':4,'interval':"E",'freq':4},
           {'ligne':5,'interval':"A",'freq':20},
         {'ligne':5,'interval':"B",'freq':10},
         {'ligne':5,'interval':"C",'freq':8},
         {'ligne':5,'interval':"D",'freq':10},
         {'ligne':5,'interval':"E",'freq':5},
           {'ligne':6,'interval':"A",'freq':20},
         {'ligne':6,'interval':"B",'freq':8},
         {'ligne':6,'interval':"C",'freq':4},
         {'ligne':6,'interval':"D",'freq':10},
         {'ligne':6,'interval':"E",'freq':2},
           {'ligne':7,'interval':"A",'freq':20},
         {'ligne':7,'interval':"B",'freq':4},
         {'ligne':7,'interval':"C",'freq':5},
         {'ligne':7,'interval':"D",'freq':5},
         {'ligne':7,'interval':"E",'freq':2}
        ]

##########################################################
class citibike():
	def Frequency_Station(self,t,station,timeinterval):
	    end = t +timedelta(minutes=timeinterval)
	    query = colmetro.find({"stop_id":station,"DatetimeNYC": {"$gte": t, "$lt": end}})
	    metrocount = query.count()
	    #return "A la station %s, il y a eu %s train\n" "(Entre %s et %s)\n"%(station,trainParQH,start,end)
	    return metrocount

	def ClosestMetroFreq(self,t,MetroListe,timeinterval):
	    #L = self.ClosestMetro(bikestation)
	    L =MetroListe
	    L2=[]
	    for i in L:
		a=str(i)+"S"
		b=str(i)+"N"
		L2.append(a)
		L2.append(b)
	    somme = sum(map(lambda x: self.Frequency_Station(t,str(x)), L2,timeinterval))
	    moyenne = somme/float(len(L2))
	    return moyenne

	def set_interval(self,t):
	    midnight = t.replace(hour=0, minute=0, second=0, microsecond=0)
	    sixthirtyam = t.replace(hour=6, minute=30, second=0, microsecond=0)
	    ninethirtyam= t.replace(hour=9, minute=30, second=0, microsecond=0)
	    threethirtypm= t.replace(hour=15, minute=30, second=0, microsecond=0)
	    eightpm = t.replace(hour=20, minute=0, second=0, microsecond=0)
	    
	    holidays= [(5,30),(7,4),(9,5),(11,11),(11,24)]
	    f= t.weekday()
	    if t > midnight and t<=sixthirtyam:
		return "A"
	    elif f==5 or f==6:
		return "B"
	    elif (t.month,t.day) in holidays:
		return "B"
	    elif f!=5 and f!=6:
		if t>ninethirtyam and t<=threethirtypm:
		    return "C"
		elif t>eightpm:
		    return "D"
		else:
		    return "E"

	def Frequency_Station_Headway(self,t,station,timeinterval):
	    ligne = int(str(station[0]))
	    inter = self.set_interval(t)
	    q= filter(lambda x: x['ligne']==ligne and x['interval'] == inter , passage_metro_prevu)
	    return timeinterval/float(q[0]['freq'])


	def CoefPerturbationMetro(self,t,MetroListe,timeinterval):
	    try:
		    #L = self.ClosestMetro(bikestation,distance)
		    L =MetroListe
		    L2=[]
		    for i in range(len(L)):
			L2.append(L[i])
			L2.append(L[i])
		    L3=[]
		    for i in L:
			a=str(i)+"S"
			b=str(i)+"N"
			L3.append(a)
			L3.append(b)
		    FP=map(lambda x: self.Frequency_Station_Headway(t,str(x),timeinterval), L2)
		    FR=map(lambda x: self.Frequency_Station(t,str(x),timeinterval), L3)
		    #coefStation = map(lambda x,y : x/y for x in FR and y in FP)
		    coefStation = [x/y for x,y in zip(FR,FP)]
		    somme = sum(coefStation)
		    moyenne = somme/float(len(L3))
		    return moyenne
	    except:
	    	return 'NaN'

	def CoefPerturbationTrafic(t,ArcListe,timeinterval):
	####import des tables via les csv########################


	    dfCarSpeedMain = pd.DataFrame.from_csv('CarSpeedMain_Average_CLN.csv',header=0, sep=',',index_col=None)
	    #DFinters = pd.DataFrame.from_csv('Intersection_Trafic_BikeStation.csv',header=0, sep=',',index_col=None)
	     #########################################################
	    dfCarSpeedListe = pd.DataFrame(columns=['DataAsOf','linkId','Speed','AverageSpeed'])

	    #ArcListe = DFinters[DFinters.BikeStation==bikestation]['Arc'].tolist()
	    end = t +timedelta(minutes=timeinterval)
	    #print t,end
	    for arc in ArcListe:

		query = colcarSpeed.find({"linkId": int(arc),'DataAsOf':
		      {"$gte": t, "$lt": end}},{'DataAsOf':1,'linkId':1,'Speed':1})

		dfCarSpeed= DataFrame(list(query),columns = ['DataAsOf','linkId','Speed'])
		#print dfCarSpeed
		j = dfCarSpeed['Speed'].mean()
		u = dfCarSpeedMain[dfCarSpeedMain.linkId==arc]['AverageSpeed'].values[0]
		#print st, j,u
		#if u>0:
		Liste = [t,arc,j,u]
		#print Liste
		dfCarSpeedListe.loc[-1] = Liste
		dfCarSpeedListe.index = dfCarSpeedListe.index + 1
	    dfCarSpeedListe['linkId'] = dfCarSpeedListe['linkId'].astype(int)
	    dfCarSpeedListe['CoefTrafic']= dfCarSpeedListe['Speed']/dfCarSpeedListe['AverageSpeed']
	    dfCarSpeedListe.dropna(subset=['Speed'])
	    #print dfCarSpeedListe

	    return dfCarSpeedListe['CoefTrafic'].mean()
