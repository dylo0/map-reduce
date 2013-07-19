import MapReduce
import sys

"""
Matrix multiplication in MapReduce
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # multiplication of hardcoded 5x5 matrix!
    if record[0] == "a":
      for k in range(5):
        mr.emit_intermediate((record[1], k), record)
    else:
      for j in range(5):
        mr.emit_intermediate((j, record[2]), record)


def reducer(key, list_of_values):
    A = []
    B = []
    # appends vectors A, B = numer + value
    for value in list_of_values:
      if value[0] == "a":
        A.append((value[2], value[3]))
      else:
        B.append((value[1], value[3]))

    values_sum = 0
    # vector dotproduct 
    for i in A:
      for k in B:
        if i[0] == k[0]:
          values_sum += (i[1] * k[1])

    mr.emit((key[0], key[1], values_sum))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
