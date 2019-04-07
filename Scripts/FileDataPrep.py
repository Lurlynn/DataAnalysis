from collections import Counter, defaultdict
from itertools import groupby, chain
from pandas import DataFrame
import gzip, json, time, os, psutil, csv, math
import pandas as pd



def processFiles():
    i = 0
    dc={}
    #Data for Elbow and Kmeans as well as linear regression
    print("Prepping for Kmeans")
    score = ""
    year = ""
    input_file = open('reviewText.txt','r')
    for line in input_file:
        information = line.split(';;; ')
        word_length = len(information[7].split())
        if (int(information[5]) > 0):
            score = information[9].strip()
            year = (information[11][-5:]).strip('\n')
            if (score == '1.0' or score == '2.0' or score == '3.0' or score == '4.0' or score == '5.0'):
                dc[information[3]] = {"word_count": word_length, "asin": information[1],"helpful": int(information[5]),"count": 1,"score": float(score), "year":year}
        if (i % 500000 == 0):
            print ("dict processed "+ str(i))
        i+=1
    input_file.close()


    list_of_dics2 = [value for value in dc.values()]

    df = pd.DataFrame(list_of_dics2)
    g = df.groupby('asin').agg({'count': 'count', 'helpful':'sum', 'word_count': 'sum'}).reset_index()
    d = g.to_dict('r')
    text_file = open("ReviewData.txt", "w")
    for v in d:
         text_file.write(str(v['helpful']/v['count']) +","+ str(v['word_count']/v['count'])+'\n')
    text_file.close()

    del(d)
    del(dc)
    del(list_of_dics2)

    print("Prepping for Linear")
    #linear regression 1
    g1 = df.groupby('asin').agg({'count': 'count', 'helpful':'sum', 'score': 'sum'}).reset_index()
    d1 = g1.to_dict('r')
    text_file = open("ScoreHelpfulData.txt", "w")
    for v in d1:
         text_file.write(str(v['score']/v['count']) +","+ str(v['helpful']/v['count'])+'\n')
    text_file.close()
    del(d1)

    #linear regression 2
    g1 = df.groupby('asin').agg({'count': 'count', 'helpful':'sum', 'score': 'sum'}).reset_index()
    d1 = g1.to_dict('r')
    text_file = open("HelpfulScoreData.txt", "w")
    for v in d1:
         text_file.write( str(v['helpful']/v['count'])+","+ str(v['score']/v['count'])+'\n')
    text_file.close()
    del(d1)

    print("Prepping for 1st bar graph")
    #Histogram Data for trending in year
    g2 = df.groupby('year').agg({'count': 'count'}).reset_index()
    d2 = g2.to_dict('r')
    text_file = open("YearData.txt", "w")
    for v in d2:
         text_file.write(str(v['year']) +","+ str(v['count'])+'\n')
    text_file.close()
    del(d2)

    print("Prepping for 2nd bar graph ")
    #Histogram Data for trending in year
    g2 = df.query('helpful >= 4.0').groupby('year').agg({'count': 'count'}).reset_index()
    d2 = g2.to_dict('r')
    text_file = open("HighratingData.txt", "w")
    for v in d2:
         text_file.write(str(v['year']) +","+ str(v['count'])+'\n')
    text_file.close()
    del(d2)

def main():
    processFiles()

if __name__ == '__main__':
    main()

