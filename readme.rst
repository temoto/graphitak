What
====

Text generator that uses real language corpus to produce close to natural texts.

Current algorithm:
* use word from special list to open a sentence
* use one of top 1000 3-grams to complete last word
* repeat from step 2 until required number of words is added to current phrase (sentence)
* repeat from step 1 until required number of phrases is collected in paragraph

3-grams and special list of first words are stored in PostgreSQL.


TODO
====

* implement myrexp() exponential distribution random in Python
* inflect words at boundaries to appropriate forms
* use SQLite? to store n-grams
