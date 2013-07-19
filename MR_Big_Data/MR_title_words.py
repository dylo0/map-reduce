import mincemeat
import glob

"""
my first map-reduce script, made for assignment in 'Web Intelligence and Big Data' IITD course 
counts #words in book titles used by each author, omits 'stopwords'
liblary data provided in separated files like example one.
"""

text_files = glob.glob('hw3data\*')

def file_contents(file_name):
    f = open(file_name)
    try:
        return f.read()
    finally:
        f.close()

source = dict((file_name, file_contents(file_name)) for file_name in text_files)


def mapfn(key, value):
    import re  #have to be inside each mapper
    allStopWords = ['all', 'herself', 'should', 'to', 'only', 'under', 'do', 'weve', 'very', 'cannot', 'werent', 'yourselves', 'him', 'did',
     'these', 'she', 'havent', 'where', 'whens', 'up', 'are', 'further', 'what', 'heres', 'above', 'between', 'youll', 'we', 'here', 'hers',
      'both', 'my', 'ill', 'against', 'arent', 'thats', 'from', 'would', 'been', 'whos', 'whom', 'themselves', 'until', 'more', 'an', 'those',
       'me', 'myself', 'theyve', 'this', 'while', 'theirs', 'didnt', 'theres', 'ive', 'is', 'it', 'cant', 'itself', 'im', 'in', 'id', 'if', 'same',
        'how', 'shouldnt', 'after', 'such', 'wheres', 'hows', 'off', 'i', 'youre', 'well', 'so', 'the', 'yours', 'being', 'over', 'isnt', 'through',
         'during', 'hell', 'its', 'before', 'wed', 'had', 'lets', 'has', 'ought', 'then', 'them', 'they', 'not', 'nor', 'wont', 'theyre', 'each', 'shed',
          'because', 'doing', 'some', 'shes', 'our', 'ourselves', 'out', 'for', 'does', 'be', 'by', 'on', 'about', 'wouldnt', 'of', 'could', 'youve', 'or',
           'own', 'whats', 'dont', 'into', 'youd', 'yourself', 'down', 'doesnt', 'theyd', 'couldnt', 'your', 'her', 'hes', 'there', 'hed', 'their', 'too',
            'was', 'himself', 'that', 'but', 'hadnt', 'shant', 'with', 'than', 'he', 'whys', 'below', 'were', 'and', 'his', 'wasnt', 'am', 'few', 'mustnt',
             'as', 'shell', 'at', 'have', 'any', 'again', 'hasnt', 'theyll', 'no', 'when', 'other', 'which', 'you', 'who', 'most', 'ours ', 'why', 'having',
              'once']

    for line in value.splitlines():
        entry = line.split(":::")
        wordList = {}
        for word in re.findall('(\w+|[a-zA-Z]+)', entry[2]):
            if len(word) == 1 or word in allStopWords:
                continue
            else:
                if word.lower() in wordList:
                    wordList[word.lower()] += 1
                else:
                    wordList[word.lower()] = 1

        autors = entry[1].split("::")
        for autor in autors:
            yield autor, w
        

def reducefn(key, values):
    wordList = {}
    for value in values:
        for key in value:
            if key in wordList:
                wordList[key] += value[key]
            else:
                wordList[key] = value[key]
    return key, wordList

#runs server
s = mincemeat.Server()
s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme")

#writes results
with open("output.txt", "a") as myfile:
    myfile.write(str(results))
