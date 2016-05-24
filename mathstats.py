#!/usr/bin/env python2.7
import mincemeat
import math
import sys

# Don't forget to start a client!
# ./mincemeat.py -l -p changeme
filename = sys.argv
file = open(filename[1],'r')
data = list(file)
file.close()


# The data source can be any dictionary-like object
datasource = dict(enumerate(data))

def mapfn(k, v):
    for w in v.split():
        yield 'count', 1
        yield 'sum',int(w)
        yield 'stddev',int(w)


def reducefn(k, vs):
    import math
    s=0
    totallist = []
    mean=0
    standard_dev=0

    if k!='count' and k!='sum':
      for var1 in vs:
         totallist.append(var1)
      #calculate the mean
      mean=float(sum(totallist))/float(len(totallist))
      dev=[]
      #difference between the number and mean
      for x in totallist:
           dev.append(x-mean)
      sqr=[]
      #square of the difference
      for x in dev:
          sqr.append(x*x)
      
      #standard deviation
      standard_dev= math.sqrt(sum(sqr)/(len(sqr))) 
      return standard_dev
          
    if k!='sum' and k!='stddev':    
        count=sum(vs)
        return count

    if k!='count' and k!='stddev':    
        count=sum(vs)
        return count
    
s = mincemeat.Server()
s.datasource = datasource
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme")

print 'Count: ',results['count']
print "Sum: ",results['sum']
print 'stddev:',results['stddev']
