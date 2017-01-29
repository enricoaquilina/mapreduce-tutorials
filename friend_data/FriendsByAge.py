from mrjob.job import MRJob

class MRAverageAgeCounter(MRJob):
    def mapper(self, key, line):
        (userID, userName, age, noOfFriends) = line.split(',')
        yield int(age), int(noOfFriends)

    def reducer(self, age, noOfFriends):
        # the looping across the various ages is implicit..we define what we do with each age
        # age#1: 45, 23, age#2: 90, 20, age#3: 100, 2
        total = 0
        numElements = 0
        for x in noOfFriends:
            total += x
            numElements += 1

        yield age, total / numElements

if __name__ == '__main__':
    MRAverageAgeCounter.run()