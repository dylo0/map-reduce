import MapReduce
import sys

"""
Returns all asymmetric friendship pairs
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # sorts pairs so it will be assigned to the same reduce bucket
    key = sorted(record)
    mr.emit_intermediate((key[0],key[1]), record)

def reducer(key, list_of_values):
    # checks if pair is asymmetric, if so it emits 2 pairs - normal and inverted
    person = list_of_values[0][0]
    assymetric = False
    for pair in list_of_values:
      if pair[1] == person:
        assymetric = True
        break

    if not assymetric:
      mr.emit(key)
      mr.emit((key[1],key[0]))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
