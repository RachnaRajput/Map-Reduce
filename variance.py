import MapReduce
import sys
import json
import os
import string
import decimal
from decimal import *
getcontext().prec = 16
"""
calculate the variance of the number

"""
mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):

    count = (len(record))
    sum = 0
    sqsum = 0
    for i in range(0,len(record)):
        sum = sum + record[i]
        sqsum = sqsum + pow(record[i],2)

    mr.emit_intermediate((1), (sum,sqsum,count))

def reducer(key, list_of_values):
     sum = 0
     sqsum = 0
     countsum = 0
     for str in list_of_values:
         sum = sum + str[0]
         sqsum = sqsum + str[1]
         countsum = countsum + str[2]

     result =  (+Decimal((sqsum/float(countsum))-pow((sum/float(countsum)),2)))
     print (result)


# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open("C:\\Users\\Rachna\\Downloads\\demo\\variance.json")
  mr.execute(inputdata, mapper, reducer)
