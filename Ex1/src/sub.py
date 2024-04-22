from mrjob.job import MRJob
import json

'''
This file contains the map-reduce job, that counts the number of reviews per category.
'''
class CategoryCounter(MRJob):

    # in the mapper we yield for each review its category and 1: [category,1]
    def mapper(self, _, line):
        review = json.loads(line)
        category = review['category']
        yield category,1

    # a combiner is used to reduce the data that is sent between the nodes
    def combiner(self, category, counts):
        yield category, sum(counts)

    # in the reducer the numbers are summed up and then yielded
    def reducer(self, category, counts):
        yield category, sum(counts)


if __name__ == '__main__':
    CategoryCounter.run()


