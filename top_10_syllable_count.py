
from mrjob.job import MRJob
from mrjob.step import MRStep

import re
from sys import stderr

import syllables


# See the note above about debugging
def debug(*msg, **kwargs):
    """Print debugging message to standard error."""
    print(*msg, file=stderr, **kwargs)
    

def splitter(text):
    WORD_RE = re.compile(r"[\w']+")
    return WORD_RE.findall(text)


def sort_results(results):
    """
    Sorts a list of 2-tuples descending by the first value in the 
    tuple, ascending by the second value in the tuple.
    """
    return sorted(results, key=lambda k: (-k[0], k[1]))


STOPWORDS = {
    'i', 'we', 'ourselves', 'hers', 'between', 'yourself', 'but', 'again', 'there', 'about', 'once', 'during',
    'out', 'very', 'having', 'with', 'they', 'own', 'an', 'be', 'some', 'for', 'do', 'its', 'yours', 'such',
    'into', 'of', 'most', 'itself', 'other', 'off', 'is', 's', 'am', 'or', 'who', 'as', 'from', 'him', 'each',
    'the', 'themselves', 'until', 'below', 'are', 'we', 'these', 'your', 'his', 'through', 'don', 'nor', 'me',
    'were', 'her', 'more', 'himself', 'this', 'down', 'should', 'our', 'their', 'while', 'above', 'both', 'up',
    'to', 'ours', 'had', 'she', 'all', 'no', 'when', 'at', 'any', 'before', 'them', 'same', 'and', 'been',
    'have', 'in', 'will', 'on', 'does', 'yourselves', 'then', 'that', 'because', 'what', 'over', 'why', 'so',
    'can', 'did', 'not', 'now', 'under', 'he', 'you', 'herself', 'has', 'just', 'where', 'too', 'only', 'myself',
    'which', 'those', 'i', 'after', 'few', 'whom', 't', 'being', 'if', 'theirs', 'my', 'against', 'a', 'by',
    'doing', 'it', 'how', 'further', 'was', 'here', 'than'
}


# YOUR CODE HERE
class MRSyllableCounter(MRJob):

    def mapper(self, _, line):
        words = splitter(line)
        for word in words:
            word_lower = word.lower()
            if word_lower not in STOPWORDS:
                yield word_lower, syllables.estimate(word_lower)

    def combiner(self, word, counts):
        yield word, max(counts)

    def reducer(self, word, counts):
        yield None, (max(counts), word)

    def result_sorter(self, _, word_info):
        word_info = sort_results(list(word_info))
        top10 = word_info[:10]
        for word in top10:
            yield word

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
            MRStep(reducer=self.result_sorter)
        ]

if __name__ == '__main__':
    MRSyllableCounter.run()
