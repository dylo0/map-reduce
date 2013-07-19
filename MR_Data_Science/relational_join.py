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
    key = record[1]
    mr.emit_intermediate(key, record)


def reducer(key, records):
    # key: word
    # value: list of occurrence counts
    orders = []
    items = []
    joined_record = []
    for record in records:
      if record[0] == "order":
        orders.append(record)
      else:
        items.append(record)
    
    for order in orders:
      for item in items:
        mr.emit(order+item)


# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
