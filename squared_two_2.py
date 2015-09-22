import MapReduce
import sys
import re

mr = MapReduce.MapReduce()


# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    key=record[0],record[1]
    val=record[2]
    mr.emit_intermediate(key,val)


def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    val=sum(list_of_values)
    mr.emit((key[0],key[1],val))



# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
