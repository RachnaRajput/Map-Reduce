import MapReduce
import sys
import json
import os

"""
Social Network; Finding connection

"""
mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line
# friendlist = {}

def mapper(record):
     friend1 = record[0]
     friend2 = record[1]

     mr.emit_intermediate(friend1,friend2)
     mr.emit_intermediate(friend2,friend1)

def reducer(key, value):
    friendlist = {}
    if key not in friendlist:
         friendlist[key] = value
    else:
         name = friendlist[key]
         friendlist[key] = name + "," + value

    for x in friendlist:
        mylist = friendlist[x]

        mykey=x.decode("utf-8")
        mykey=mykey.encode("ascii","ignore")
        if(len(mylist)>1):
            mylist = sorted(mylist)
            for i in range(1,len(mylist)):
                j=i-1
                while(j >= 0):
                    data1=mylist[j]
                    udata1=data1.decode("utf-8")
                    asciidata1=udata1.encode("ascii","ignore")

                    data2=mylist[i]
                    udata2=data2.decode("utf-8")
                    asciidata2=udata2.encode("ascii","ignore")
                    print (asciidata1,asciidata2,mykey)
                    j = j-1




# Do not modify below this line
# =============================
if __name__ == '__main__':
  print("hello................")
  inputdata = open("C:\Users\Rachna\Downloads\demo\socialnetwrok.json")
  mr.execute(inputdata, mapper, reducer)