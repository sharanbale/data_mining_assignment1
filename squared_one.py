import MapReduce
import sys
import re

mr = MapReduce.MapReduce()


# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    i=record[0]
    j=record[1]
    v=record[2]
    for k in range(0,5):
        keya=i,k
        keyb=k,j
        vala='A',i,j,v
        valb='B',i,j,v
        mr.emit_intermediate(keya, vala)
        mr.emit_intermediate(keyb, valb)


def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    mat={}
    for item in list_of_values:
        t=item[1],item[2]
        if t in mat:
            continue
        else:
            mat[t]=item[3]
    s=0
    i=key[0]
    k=key[1]
    for j in range(0,5):
        if (i,j) not in mat:continue
        if (j,k) not in mat:continue
        s=s+(mat[i,j]*mat[j,k])
    mr.emit((key[0],key[1],s))


# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
