from mrjob.job import MRJob

class MRAverageAgeCounter(MRJob):
    def mapper(self, key, line):
        (user_id, username, age, no_of_friends) = line.split(',')
        yield int(age), int(no_of_friends)

    def reducer(self, age, no_of_friends):
        # the looping across the various ages is implicit..we define what we do with each age
        # age#1: 45, 23, age#2: 90, 20, age#3: 100, 2
        total = 0
        num_elements = 0
        for x in no_of_friends:
            total += x
            num_elements += 1

        yield age, total / num_elements

if __name__ == '__main__':
    MRAverageAgeCounter.run()