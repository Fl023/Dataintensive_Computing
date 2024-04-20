from mrjob.job import MRJob
import json

class CategoryCounter(MRJob):

    def mapper(self, _, line):
        review = json.loads(line)
        category = review['category']
        yield category,1

    def combiner(self, category, counts):
        yield category, sum(counts)

    def reducer(self, category, counts):
        yield category, sum(counts)


if __name__ == '__main__':
    CategoryCounter.run()


