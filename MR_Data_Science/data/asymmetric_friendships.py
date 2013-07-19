import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    # print record
    # key = record.sort()
    # print key
 #   mr.emit_intermediate(sorted(record), record)
    mr.emit_intermediate(record[0], record)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    person = list_of_values[0][0]
    assymetric = True
    # for pair in list_of_values:
    #   if pair[1] == person:
    #     assymetric = True
    #     break

    if assymetric:
      mr.emit(key)
      # mr.emit(key[1],key[0])

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
