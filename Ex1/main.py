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

        stopwords = {'therefore', 'sorry', 'j', 'obviously', 'here', 'asking', 'across', 'their', 'life', 'towards', 'theirs', 'way', 'needs', 'secondly', 'respectively', 'plus', 'noone', 'very', 'books', 'yourself', 'qv', 'say', 'now', 'soon', 'am', 'seeing', 'm', 'non', 'zero', 'elsewhere', 'there', 'that', 'one', 'behind', 'case', 'overall', 'own', 'she', 'co', 'concerning', 'himself', 'hardly', 'song', 'lately', 'whereupon', 'un', 'hair', 'rather', 'selves', 'outside', 'exactly', 'thank', 'seems', 'sub', 'ought', 'presumably', 'herein', 'were', 'except', 'sup', 'beyond', 'mainly', 'inward', 'whenever', 'ourselves', 'vs', 'eg', 'contain', 'third', 'off', 'greetings', 'up', 'being', 'forth', 'theres', 'keeps', 'll', 'down', 'doing', 'five', 'thereafter', 'came', 'shall', 'z', 'only', 'lest', 'thereby', 'c', 'really', 'when', 'indicates', 'self', 'someone', 'unfortunately', 'namely', 'not', 'thanks', 'hasn', 'useful', 'especially', 'since', 'becoming', 'd', 'may', 'follows', 'et', 'need', 'who', 'getting', 'want', 'can', 'shoes', 'yet', 'before', 'ain', 'stroller', 'former', 'l', 'mean', 'ignored', 'whose', 'appear', 'install', 'both', 'seem', 'instead', 'reasonably', 'two', 'h', 'app', 'everywhere', 'our', 'normally', 'comes', 'nobody', 'likely', 'besides', 'unto', 'so', 'had', 'kitchen', 'right', 'new', 'taste', 'quite', 'relatively', 'three', 'probably', 'also', 'certainly', 'indeed', 'no', 'otherwise', 'thru', 'taken', 'little', 'willing', 'beforehand', 'became', 'far', 'therein', 'aa', 'upon', 'f', 'allow', 'from', 'somewhat', 'songs', 'let', 'many', 'film', 'wish', 'be', 'thereupon', 'value', 'become', 'better', 'k', 'placed', 'thanx', 'serious', 'everybody', 'novel', 'x', 'few', 'gives', 'further', 'says', 'the', 'truly', 'themselves', 'some', 'flavor', 'tell', 'herself', 'among', 'regarding', 'thoroughly', 'because', 'despite', 'merely', 'them', 'through', 'v', 'apart', 'never', 'was', 'else', 'ex', 'okay', 'several', 'baby', 'gotten', 'viz', 'itself', 're', 'doesn', 'dogs', 'another', 'throughout', 'above', 'liked', 'after', 've', 'such', 'q', 'cannot', 'do', 'tried', 'perhaps', 'maybe', 'for', 'anywhere', 'mower', 'changes', 'y', 'as', 'together', 'nowhere', 'accordingly', 'out', 'didn', 'unlikely', 'nine', 'took', 'u', 'ever', 'everyone', 'g', 'guitar', 'less', 'appreciate', 'away', 'available', 'following', 'at', 'amongst', 'yes', 'somehow', 'definitely', 'still', 'know', 'nearly', 'within', 'whom', 'hereby', 'ink', 'howbeit', 'of', 'enough', 'said', 'whole', 'car', 'downwards', 'along', 'what', 'with', 'next', 'four', 'couldn', 'help', 'everything', 'something', 'lamp', 'anybody', 'n', 'movie', 'whether', 'various', 'trying', 'aren', 'meanwhile', 'then', 'going', 'once', 'how', 'laptop', 'in', 'happens', 'wear', 'yours', 'hereupon', 'seen', 'will', 'thus', 'on', 'different', 'knife', 'inasmuch', 'although', 'weren', 'isn', 't', 'story', 'us', 'like', 'none', 'whereby', 'followed', 'toy', 'able', 'more', 'hi', 'old', 'allows', 'beside', 'myself', 'latter', 'absorbs', 'bulbs', 'whence', 'over', 'afterwards', 'anyway', 'all', 'however', 'particularly', 'ie', 'somebody', 'look', 'whereas', 'somewhere', 'others', 'six', 'to', 'have', 'he', 'specified', 'been', 'fifth', 'strings', 'whereafter', 'uses', 'necessary', 'though', 'why', 'which', 'printer', 'welcome', 'sure', 'we', 'you', 'second', 'anyways', 'while', 'thorough', 'per', 'between', 'anyhow', 'example', 'hers', 'under', 'most', 'ok', 'seemed', 'que', 'later', 'me', 'almost', 'indicate', 'ours', 'containing', 'hither', 'every', 's', 'wonder', 'might', 'your', 'take', 'thats', 'course', 'th', 'must', 'him', 'and', 'got', 'immediate', 'first', 'coffee', 'thence', 'indicated', 'wouldn', 'brief', 'knows', 'sometime', 'particular', 'o', 'by', 'least', 'skin', 'yourselves', 'ltd', 'or', 'about', 'is', 'please', 'see', 'nothing', 'doll', 'into', 'inc', 'goes', 'rd', 'anyone', 'cause', 'keep', 'known', 'gets', 'where', 'any', 'given', 'does', 'this', 'e', 'believe', 'looks', 'oh', 'but', 'think', 'don', 'last', 'went', 'already', 'onto', 'shave', 'according', 'haven', 'b', 'used', 'always', 'name', 'dog', 'than', 'considering', 'her', 'sensible', 'nor', 'provides', 'without', 'causes', 'mon', 'use', 'well', 'kept', 'again', 'near', 'hopefully', 'my', 'specifying', 'whatever', 'bibs', 'these', 'twice', 'each', 'get', 'phone', 'wherein', 'alone', 'try', 'grill', 'hello', 'best', 'tends', 'nevertheless', 'below', 'associated', 'having', 'ask', 'hereafter', 'around', 'regardless', 'latterly', 'consequently', 'awfully', 'looking', 'cd', 'i', 'truck', 'specify', 'cant', 'hadn', 'entirely', 'could', 'eight', 'much', 'fun', 'mostly', 'via', 'whither', 'its', 'those', 'contains', 'often', 'it', 'toward', 'a', 'appropriate', 'currently', 'his', 'during', 'shouldn', 'wherever', 'nd', 'bb', 'book', 'album', 'sent', 'possible', 'insofar', 'saw', 'inner', 'etc', 'game', 'are', 'certain', 'should', 'moreover', 'actually', 'sometimes', 'just', 'formerly', 'against', 'they', 'hence', 'seven', 'aside', 'becomes', 'wants', 'either', 'come', 'gone', 'has', 'com', 'even', 'seriously', 'other', 'would', 'saying', 'described', 'go', 'won', 'product', 'clearly', 'accord', 'edu', 'usually', 'r', 'whoever', 'an', 'p', 'using', 'furthermore', 'wasn', 'regards', 'camera', 'anything', 'bike', 'if', 'same', 'unless', 'corresponding', 'consider', 'ones', 'neither', 'read', 'seeming', 'did', 'done', 'until', 'tries', 'too'}

        filtered_tokens = set(word for word in tokens if word not in stopwords and len(word) > 1)

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



