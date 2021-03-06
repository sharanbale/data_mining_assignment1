import MapReduce
import sys
import re

mr = MapReduce.MapReduce()


# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    l = len(record)
    pd = {}
    for i in range(0, l):
        for j in range(i + 1, l):
            p=record[i],record[j]
            #p=str(record[i])+','+str(record[j])
            pd[p] = 1
    for pairs in pd:
        mr.emit_intermediate(pairs, 1)


def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    nop = len(list_of_values)
    if nop >= 100:
        mr.emit((key))


# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
