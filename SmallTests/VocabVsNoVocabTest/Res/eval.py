import matplotlib.pyplot as plt
import numpy as np
from operator import add

RESULT_FILE = './results.txt'

data = {}
data['total_terms'] = []
## size
data['bytes_fullterm'] = []
data['bytes_termsec'] = []
data['bytes_termsec_big'] = []
data['bytes_vocab_big'] = []
data['bytes_vocab'] = []
## time
data['time_no_vocab_trump'] = []
data['time_int_trump'] = []
data['time_big_int_trump'] = []
data['time_no_vocab_pres'] = []
data['time_int_pres'] = []
data['time_big_int_pres'] = []

partials = [1, 5, 10, 50, 100]

for line in open(RESULT_FILE, 'r'):
    line = line.split(' ')
    if line[0] == 'TotalTerms:':
        data['total_terms'].append(int(line[1]))
    elif len(line) > 4 and line[2] == "'fullterm',":
        data['bytes_fullterm'].append(float(line[4].replace(',', '')))
    elif len(line) > 4 and line[2] == "'termsec',":
        data['bytes_termsec'].append(float(line[4].replace(',', '')))
    elif len(line) > 4 and line[2] == "'termsec_big',":
        data['bytes_termsec_big'].append(float(line[4].replace(',', '')))
    elif len(line) > 4 and line[2] == "'vocab_big',":
        data['bytes_vocab_big'].append(float(line[4].replace(',', '')))
    elif len(line) > 4 and line[2] == "'vocab',":
        data['bytes_vocab'].append(float(line[4].replace(',', '')))
    elif line[0] == "trumpExecuteNoVocab:":
        data['time_no_vocab_trump'].append(float(line[1]))
    elif line[0] == "trumpExecuteVocabIntSearch:":
        data['time_int_trump'].append(float(line[1]))
    elif line[0] == "trumpExecuteVocabBigIntSearch:":
        data['time_big_int_trump'].append(float(line[1]))
    elif line[0] == "presidentExecuteNoVocab:":
        data['time_no_vocab_pres'].append(float(line[1]))
    elif line[0] == "presidentExecuteVocabIntSearch:":
        data['time_int_pres'].append(float(line[1]))
    elif line[0] == "presidentExecuteVocabBigIntSearch:":
        data['time_big_int_pres'].append(float(line[1]))

print(data)

unit = 1048576
bytes_fullterm = data['bytes_fullterm']
bytes_fullterm = [(bytes/unit) for bytes in bytes_fullterm]
bytes_vocab = list( map(add, data['bytes_termsec'], data['bytes_vocab']))
bytes_vocab = [(bytes/unit) for bytes in bytes_vocab]
bytes_bigvocab = list( map(add, data['bytes_termsec_big'], data['bytes_vocab_big']))
bytes_bigvocab = [(bytes/unit) for bytes in bytes_bigvocab]

plt.figure(0)

width = 0.23
ind = np.arange(len(bytes_fullterm)) 
plt.bar(ind, bytes_fullterm, width, label='NoVocab')
plt.bar(ind + width, bytes_vocab, width, label='IntVocab')
plt.bar(ind + 2*width, bytes_bigvocab, width, label='BigIntVocab')
plt.ylabel('memory [MB]')
plt.xlabel('percentage of dataset [%]')

plt.xticks(ind + (3*width / 3), partials)
plt.legend(loc='best')
plt.savefig("compMem.pdf", dpi=100, format="pdf")
plt.show()

def autolabel(rects1, rects2, rects3, text):
    """Attach a text label above each bar in *rects*, displaying its height."""
    for i in range(0, len(rects1)):
        x = ((rects1[i].get_x() + rects1[i].get_width() / 2 ) + (rects2[i].get_x() + rects2[i].get_width() / 2 ) + (rects3[i].get_x() + rects3[i].get_width() / 2 )) / 3
        height = max([rects1[i].get_height(), rects2[i].get_height(), rects3[i].get_height()])
        plt.annotate('{}'.format(text), xy=(x, height), 
                        xytext=(0, 3), textcoords="offset points", ha='center', va='bottom', size=6)

plt.figure(1)
width = 0.11
innerwidth = 0.06
ind = np.arange(len(bytes_fullterm))
rects1 = plt.bar(ind, data['time_no_vocab_trump'], width, label='NoVocab q=trump', color='tab:blue')
rects2 = plt.bar(ind + width, data['time_int_trump'], width, label='IntVocab q=trump', color='tab:orange')
rects3 = plt.bar(ind + 2*width, data['time_big_int_trump'], width, label='BigIntVocab q=trump', color='tab:green')

rects4 = plt.bar(ind + 3*width + innerwidth, data['time_no_vocab_pres'], width, label='NoVocab q=president', color='tab:blue')
rects5 = plt.bar(ind + 4*width + innerwidth, data['time_int_pres'], width, label='IntVocab q=president', color='tab:orange')
rects6 = plt.bar(ind + 5*width + innerwidth, data['time_big_int_pres'], width, label='BigIntVocab q=president', color='tab:green')

autolabel(rects1, rects2, rects3, 'trump')
autolabel(rects4, rects5, rects6, 'president')

plt.ylabel('query execution time [ms]')
plt.xlabel('percentage of dataset [#]')

plt.xticks(ind + 2*width + 1.5*innerwidth, partials)
plt.legend(loc='best')
plt.savefig("compPerf.pdf", dpi=100, format="pdf")
plt.show()
