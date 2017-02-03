from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMostPopularHero(MRJob):
    def configure_options(self):
        super(MRMostPopularHero, self).configure_options()
        self.add_file_option('--names', help='Path to hero names')

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_hero,
                   reducer=self.reducer_count_friends),
            MRStep(mapper=self.mapper_make_friendno_key,
                   mapper_init=self.mapper_get_hero_names,
                   reducer=self.reducer_getpopularhero)
        ]

    def mapper_get_hero(self, key, line):
        heroes = line.split()
        yield int(heroes[0]), int(len(heroes) - 1)

    def reducer_count_friends(self, hero, friends):
        yield hero, sum(friends)


    def mapper_make_friendno_key(self, hero, friend_count):
        yield None, (friend_count, self.heroes[hero])

    def mapper_get_hero_names(self):
        self.heroes = {}
        with open('names.txt') as f:
            for line in f:
                fields = line.split(' ')
                self.heroes[fields[0]] = fields[1].replace('"', '')

    # since key is none for all movies, this reducer will be called
    # once and run the max operation on the entire data set
    def reducer_getpopularhero(self, key, values):
        yield max(values)

if __name__ == '__main__':
    MRMostPopularHero.run()
