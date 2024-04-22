from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import re

'''
This file contains the map-reduce job, that calculates the chi-square values for all terms
and returns the top 75 words for each category.
It consists of two steps:
1. step: 
- mapping the lines from the input file to [token, category]
- reducing for each token to the number of occurrences per category and yielding [category, (token, chi-square value)]
2. step:
- reducing the (token, chi-square value) for all categories, so that we can find the top 75 terms
'''


class ChiSquaredCalculator(MRJob):

    def __init__(self, *args, **kwargs):
        super(ChiSquaredCalculator, self).__init__(*args, **kwargs)
        self.stopwords = None  # set of stopwords, as read from "stopwords.txt"
        self.category_counts = None  # dictionary of number of reviews per category, as read from "category_counts.txt"
        self.sum_reviews = None  # sum of all reviews, calculated from the category_counts dictionary

    # defining the steps as described above
    def steps(self):
        return [MRStep(mapper_init=self.mapper_init1,
                       mapper=self.mapper1,
                       reducer_init=self.reducer_init1,
                       reducer=self.reducer1),
                MRStep(reducer=self.reducer2)]

    # mapper_init1 loads the stopwords from the file "stopwords.txt"
    def mapper_init1(self):
        with open("stopwords.txt", "r") as f:
            self.stopwords = set(f.read().splitlines())

    # mapper1 loads a line of input file, does preprocessing and maps to [token, category] for each token of line
    def mapper1(self, _, line):
        # Parse the JSON line
        review = json.loads(line)

        # Extract category and review text
        category = review['category']
        review_text = review['reviewText']

        # Tokenize and case fold the review text
        tokens = re.findall(r'[^\s\d()[\]{}.!?,;:+=\-_"\'`~#@&*%€$§\\/]+', review_text.lower())

        # Filter out the stopwords and remove duplicate words with set()
        filtered_tokens = set(word for word in tokens if word not in self.stopwords and len(word) > 1)

        # yield for every token in the line
        for word in filtered_tokens:
          yield word, category

    # reducer_init1 loads the number of reviews per category, calculates the total number of reviews, and stores it
    def reducer_init1(self):
        with open('category_counts.txt', 'r') as file:
            cat_counts = {}
            for line in file:
                category, count = line.strip().split()
                cat_counts[category] = int(count)
            self.category_counts = cat_counts
            self.sum_reviews = sum(self.category_counts.values())

    '''
    reducer1 gets the shuffled key,value pairs, e.g. [term1, (category1, category1, category2, category3)].
    From this pair we know that 2 reviews of category1, 1 review of category2 and 1 review of category3 contain term1.
    Then we iterate over all the categories of term1 and for each category the chi-square value is calculated.
    We yield [category, (term, chi-square value)].
    '''
    def reducer1(self, word, categories):
        cats = [c for c in categories]
        for c in set(cats):  # iterate over all unique categories
            A = cats.count(c)  # count how often the category is contained in key,value pair
            B = len(cats) - A  # sum of occurrences of term minus A
            C = self.category_counts[c] - A  # number of reviews of category minus A
            D = self.sum_reviews - self.category_counts[c] - C  # total sum of reviews minus number of reviews of category minus C
            X = self.sum_reviews * (A*D - B*C)**2 / ((A+B)*(A+C)*(B+D)*(C+D))  # formula from lecture for chi-square
            yield c, (word, X)


    # reducer2 collects all chi-square values of a category, sorts them, and then takes the top 75 terms
    def reducer2(self, category, wordchi):
        wordchi_list = [(word, chi) for word, chi in wordchi]
        sorted_wordchi_list = sorted(wordchi_list, key=lambda x: x[1], reverse=True)
        top_words = sorted_wordchi_list[:75]
        for word, chi in top_words:
            yield category, (word, chi)

if __name__ == '__main__':
    ChiSquaredCalculator.run()



