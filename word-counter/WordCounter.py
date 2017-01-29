from mrjob.job import MRJob
import re

WORD_REGEXP = re.compile(r"[\w']+")

class MRWordCounter(MRJob):
    def mapper(self, key, line):
        # sentence = line.split('.')
        words = WORD_REGEXP.findall(line)
        for word in words:
            yield word.lower(), 1

    def reducer(self, word, noOfOccurrences):
        yield word, sum(noOfOccurrences)

if __name__ == '__main__':
    MRWordCounter.run()
