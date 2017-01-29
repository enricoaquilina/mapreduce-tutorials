from mrjob.job import MRJob

class MRRatingCounter(MRJob):
    def mapper(self, key, line):
        print(line)
        (userID, movieID, rating, timestamp) = line.split(',')
        yield rating, 1

    def reducer(self, rating, occurences):
        yield rating, sum(occurences)

if __name__ == '__main__':
    MRRatingCounter.run()
