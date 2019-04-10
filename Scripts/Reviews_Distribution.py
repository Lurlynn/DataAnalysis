from collections import Counter, defaultdict
from itertools import groupby, chain
from pandas import DataFrame
import gzip, json, time, os, psutil, csv, math
import pandas as pd

	 

start_ts = time.time();
ids = {};
userids = {};
userUseful = {};
highreview = {};
userUseful2 = {};
rates = {};
rates2 = {};
ratings = {};
prices = {};
ratings['reviews'] = []
main_ratings = {}
ratings_sort = {};
ratings_sort['reviews'] = []
i = 0;
n_useful = 0;
d_useful = 0;
new_dict = {}
s = ""
y = ""

sales = {}
rtext = {}



def extractdata():
    out = open('extractData.txt','+w')
    outfile = open('reviewText.txt','+w')
    i = 0
    #with gzip.open('reviews_Musical_Instruments.json.gz','r') as f:
    with gzip.open('aggressive_dedup.json.gz','r') as f:
        for line in f:
            parsed_json = json.loads(line)
            ids[parsed_json['asin']] = ids.setdefault(parsed_json['asin'],0) + 1;
            if 'reviewerID' in parsed_json:
                userids[parsed_json['reviewerID']] = userids.setdefault(parsed_json['reviewerID'],0) + 1
            if 'overall' in parsed_json:
                s = "asin "+ parsed_json['asin']+" reviewerID "+ parsed_json['reviewerID']+ " overall "+ str(parsed_json['overall'])
                if parsed_json['overall'] >= 4.0:
                    highreview[parsed_json['asin']] = parsed_json['overall']
            if 'helpful' in parsed_json:
                userUseful[parsed_json['reviewerID']] = userUseful.setdefault(parsed_json['reviewerID'],0) + parsed_json['helpful'][1]
                userUseful2[parsed_json['asin']] = userUseful2.setdefault(parsed_json['asin'],0) + parsed_json['helpful'][1]
            if 'reviewText' in parsed_json:
                rtext = "asin;;; "+ parsed_json['asin']+";;; reviewerID;;; "+ parsed_json['reviewerID']+ ";;; helpful;;; "+str(parsed_json['helpful'][1])+ ";;; reviewText;;; "+ str(parsed_json['reviewText'])+";;; score;;; "+str(parsed_json['overall'])+ ";;; reviewTime;;; "+ str(parsed_json['reviewTime']) 
            out.write(s+'\n')
            outfile.write(rtext+'\n')
            if (i % 500000 == 0):
                print ("{:,}".format(i) + " records processed");
            i+=1
    print ("{:,}".format(i) + " records processed");

    outfile.close()
    out.close()

    print("processing metadata")
    i = 0
    #Dealing with the PRICES
    #with gzip.open('meta_Musical_Instruments.json.gz','r') as f:
    with gzip.open('metadata.json.gz','r') as f:
        for line in f:
            parsed_json = json.loads(json.dumps(eval(line)));
            if 'price' in parsed_json:
                prices[parsed_json['asin']] = parsed_json['price']
            if (i % 500000 == 0):
                print ("{:,}".format(i) + " records processed");
            i+=1
    print ("{:,}".format(i) + " records processed");
    print("Extraction Completed")



def processDistribution():
    dct = defaultdict(list)
    dc={}
    i = 0
    input_file = open('extractData.txt','r')
    for line in input_file:
        information = line.split() # This is assuming the text file is space-delimited.
        dct[information[3]] = {information[0]: information[1], "reviewerID": information[3], "overall": information[5]}
        if (i % 500000 == 0):
            print ("dict processed "+ str(i))
        i+=1
    input_file.close()

    list_of_dics = [value for value in dct.values()]
    print("Dictionary completed")
    
    del(dct)
    
    #Finding Mode of each item
    f = lambda x: x['asin']
    dct2 = {k: Counter(d['overall'] for d in g)for k, g in groupby(list_of_dics, f)}
    del(list_of_dics)

    mean = 0
    multiple = 0
    item_median = 0
    max_rating = 0
    max_rate = 0
    skew = ""
    sums = 0

    print("Ready for distribution")
    text_file = open('Statistics.csv', 'w')
    columnTitleRow = "Product Code, Median, Mean, Mode, Distribution\n"
    text_file.write(columnTitleRow)
    median_sum = 0
    #Finding mean, mode and median of each item
    for k, v in dct2.items():
        for l,j in v.items():
            if (j>=max_rating):
                max_rating = j
                max_rate = l
            sums+=j
            mean = float(l)*j
            multiple += mean
        if (sums % 2 == 0 and sums > 2):
            median1 = sums/2 
            median2 = sums/2 - 1 
            median = (median1 + median2)/2
        else:
            median = sums/2
        for l,j in v.items():
            median_sum += j
            if (math.ceil(median) <= median_sum):
                if (round((multiple/sums),0) > float(l)):
                    skew = "Positively Skewed"
                elif (round((multiple/sums),0) < float(l)):
                    skew = "Negatively Skewed"
                else:
                    skew = "Symmetrical"
                text_file.write(str(k)+","+str(l)+ ","+str(round((multiple/sums),0))+","+str(max_rate)+","+str(skew)+'\n')
                break;
        sums=0
        median_sum=0
        multiple=0
        max_rating=0
    text_file.close()

    del(dct2)

    print("End of distribution")



def processREviews():    
    #product reviewed the most
    f = lambda x: x['asin']
    dct3 = []
    dct3.append(ids)
    
    max_item = 0
    item = ""
    for v in dct3:
        #print (k,v)
        for l,j in v.items():
            if (j>=max_item):
                max_item = j
                item = l
    print("Product "+ str(item) + " was reviewed the most. The number of times: "+ str(max_item))

    del(dct3)

    #User that gave the most reviews
    dct4 = []
    dct4.append(userids);
    max_rev = 0
    rev = ""
    for v in dct4:
        #print (k,v)
        for l,j in v.items():
            if (j>=max_rev):
                max_rev = j
                rev = l
    print("User " + str(rev)+" gave the most reviews. The number of reviews: "+ str(max_rev))

    del(dct4)

    #User that gave the most useful reviews
    dct4 = {}
    dct4.update(userids);
    dct5 = {}
    dct5.update(userUseful);
    dict2 = defaultdict(list)
    for k, v in chain(dct5.items(), dct4.items()):
        dict2[k].append(v)
    max_use = 0
    use = ""
    for k, v in dict2.items():
        #print (k,v)
        if ((len(v) == 2) and (v[1] > 0) and (v[0] > 0)):
            avg = v[0]/v[1]
            if (avg >= max_use):
                max_use = avg
                use = k
    print("The user that gave the most useful reviews: "+ str(use) + " with a value of: "+ str(max_use))

    del(dct4)
    del(dct5)


    dct7 = {}
    dct7.update(prices);
    dct6 = {}
    dct6.update(highreview);
    dict3 = defaultdict(list)
    for k, v in chain(dct6.items(), dct7.items()):
        dict3[k].append(v)
     
    max_price = 0
    item = ""
    rate = 0
    min_price = 999999999
    min_item = ""
    min_rate = 0
    for k, v in dict3.items():
        if ((len(v) == 2) and (v[1] > 0)):
            if (v[1] >= max_price):
                max_price = v[1]
                rate = v[0]
                item = k
            if (v[1] < min_price):
                min_price = v[1]
                min_rate = v[0]
                min_item = k
    print("Most expensive high review: "+ str(item) + " " + str(rate) + " "+ str(max_price))
    print("Cheapest high review: "+ str(min_item) + " " + str(min_rate) + " "+ str(min_price))

    del(dct6)
    del(dct7)
    print ("Pouplarity Report Completed");

def main():
    extractdata()
    processDistribution()
    processREviews()

if __name__ == '__main__':
    main()
    
