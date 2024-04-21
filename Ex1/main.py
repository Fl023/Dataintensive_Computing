from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import re

class ChiSquaredCalculator(MRJob):

    # def load_category_counts(self):
    #     category_counts = {}
    #     with open('category_counts.txt', 'r') as file:
    #         for line in file:
    #             category, count = line.strip().split()
    #             category_counts[category] = int(count)
    #     return category_counts
    #
    # def load_stopwords(self):
    #     with open("stopwords.txt", "r") as f:
    #         return set(f.read().splitlines())

    def __init__(self, *args, **kwargs):
      super(ChiSquaredCalculator, self).__init__(*args, **kwargs)
      self.stopwords = None
      self.category_counts = None
      self.sum_reviews = None

    def steps(self):
        return [MRStep(mapper_init=self.mapper_init1,
                       mapper=self.mapper1,
                       reducer_init=self.reducer_init1,
                       reducer=self.reducer1),
                MRStep(reducer=self.reducer2)]

    def mapper_init1(self):
        with open("stopwords.txt", "r") as f:
            self.stopwords = set(f.read().splitlines())
    def mapper1(self, _, line):
        # Parse the JSON line
        review = json.loads(line)

        # Extract category and review text
        category = review['category']
        review_text = review['reviewText']

        # Tokenize the review text
        tokens = re.findall(r'[^\s\d()[\]{}.!?,;:+=\-_"\'`~#@&*%€$§\\/]+', review_text.lower())

        filtered_tokens = set(word for word in tokens if word not in self.stopwords and len(word) > 1)

        for word in filtered_tokens:
          yield word, category

    def reducer_init1(self):
        with open('category_counts.txt', 'r') as file:
            cat_counts = {}
            for line in file:
                category, count = line.strip().split()
                cat_counts[category] = int(count)
            self.category_counts = cat_counts
            self.sum_reviews = sum(self.category_counts.values())

    def reducer1(self, word, categories):
        cats = [c for c in categories]
        for c in set(cats):
            A = cats.count(c)
            B = len(cats) - A
            C = self.category_counts[c] - A
            D = self.sum_reviews - self.category_counts[c] - C
            X = self.sum_reviews * (A*D - B*C)**2 / ((A+B)*(A+C)*(B+D)*(C+D))
            yield c, (word, X)


    def reducer2(self, category, wordchi):
        wordchi_list = [(word, chi) for word, chi in wordchi]
        sorted_wordchi_list = sorted(wordchi_list, key=lambda x: x[1], reverse=True)
        top_words = sorted_wordchi_list[:5]
        for word, chi in top_words:
            yield category, (word, chi)

if __name__ == '__main__':
    ChiSquaredCalculator.run()



