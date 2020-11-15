#%%
import matplotlib.pyplot as plt
import numpy as np

#%%
words = []
spacy = []
nltk = []
textblob = []

for line in open('./RESULTS.txt', 'r'):
    line = line.split(' ')

    if line[0] == 'num':
        words.append(int(line[-1]))
    elif line[0] == 'timeSpacy:':
        spacy.append(float(line[-1]))
    elif line[0] == 'timeNLTK:':
        nltk.append(float(line[-1]))
    elif line[0] == 'textblob:':
        textblob.append(float(line[-1]))

print(words, spacy, nltk, textblob)


plt.plot(words, spacy, label='spaCy')
plt.plot(words, nltk, label='NLTK')
plt.plot(words, textblob, label='textblob')
plt.ylabel('time in [s]')
plt.xlabel('number of words [#]')

plt.xscale('log')
plt.axes().set_aspect(0.02)
plt.legend()
plt.savefig("nlpLibsTest.pdf", dpi=100, format="pdf")
plt.show()
#plt.close(fig=fig)
