import MapReduce
import sys
import re

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    l=len(record)
    pd={}
    for i in range(0,l):
	for j in range(i+1,l):
		p=[]
		p.append(record[i])
		p.append(record[j])
		pd[p]=1
    for pairs in pd:
    	mr.emit_intermediate(word,val)
'''
    key = record[0]
    value = record[1]
    words = value.split()
    for w in words:
      mr.emit_intermediate(w, 1)
'''
def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    nop=len(list_of_values)
    if nop>=100:
    	mr.emit((key))
'''
    total = 0
    for v in list_of_values:
      total += v
    mr.emit((key, total))
'''
# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
