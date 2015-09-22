import MapReduce
import sys
import re

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    wc={}
    filename=record[0]
    txt = open(filename,'r',errors='ignore')
    for line in txt:
        listw = line.split(' ')
        for word in listw:
            if word in wc:
                wc[word]=wc[word]+1
            else:
                wc[word]=1

    for word in wc:
        val=[]
        val.append(filename,wc[word])
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
    df=len(list_of_values)
    mr.emit(())

    total = 0
    for v in list_of_values:
      total += v
    mr.emit((key, total))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
