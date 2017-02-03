from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_REGEXP = re.compile(r"[\w']+")

class MRCounterSort(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.map_get_words,
                   reducer=self.red_count_words),
            MRStep(mapper=self.map_make_counts_key,
                   reducer=self.red_output_words)
        ]

    def map_get_words(self, key, line):
        words = WORD_REGEXP.findall(line)
        for word in words:
            yield word.lower(), 1

    def red_count_words(self, word, noOfOccurrences):
        yield word, sum(noOfOccurrences)

    def map_make_counts_key(self, word, count):
        # here we need to switch the order of the key, value pair from the reducer which we get our data from
        # thus we convert the order here and pad the count so mapreduce can sort correctly
        yield '%04d'%int(count), word

    def red_output_words(self, count, words):
        for word in words:
            yield count, word

if __name__ == '__main__':
    MRCounterSort.run()
