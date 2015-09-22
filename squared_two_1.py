import MapReduce
import sys
import re

mr = MapReduce.MapReduce()


# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    i = record[0]
    j = record[1]
    v = record[2]
    mr.emit_intermediate(j, ('A', i, v))
    mr.emit_intermediate(i, ('B', j, v))


def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    mata = {}
    matb = {}
    for item in list_of_values:
        if item[0] == 'A':
            t = 'A', item[1]
            if t in mata:
                continue
            else:
                mata[t] = item[2]
        if item[0] == 'B':
            t = 'B', item[1]
            if t in matb:
                continue
            else:
                matb[t] = item[2]

    for i in range(0,5):
        if ('A',i) not in mata: continue
        for k in range(0,5):
            if('B',k) not in matb: continue
            val=mata['A',i]*matb['B',k]
            mr.emit((i,k,val))



# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
