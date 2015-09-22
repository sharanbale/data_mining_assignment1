import MapReduce
import sys
import re

mr = MapReduce.MapReduce()


# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    wc = {}
    filename = record[0]
    filec = record[1]
    listw = filec.split(' ')
    for word in listw:
        word2 = word.lower()
        if re.match('^\w+$', word2):
            if word2 in wc:
                wc[word2] = wc[word2] + 1
            else:
                wc[word2] = 1

    for word in wc:
        val = []
        val.append(filename)
        val.append(wc[word])
        mr.emit_intermediate(word, val)


def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    df = len(list_of_values)
    mr.emit((key, df, list_of_values))


# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
