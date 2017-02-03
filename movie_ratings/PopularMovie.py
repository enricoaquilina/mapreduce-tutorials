from mrjob.job import MRJob
from mrjob.step import MRStep

class MRPopularMovie(MRJob):
    def configure_options(self):
        super(MRPopularMovie, self).configure_options()
        self.add_file_option('--items', help='Path to movies.csv')

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_movie,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reducer_getpopularmovie)
        ]

    def mapper_get_movie(self, key, line):
        (user, movie, rating, timestamp) = line.split(',')
        yield movie, 1

    def reducer_init(self):
        self.movie_names = {}
        with open('movies.csv') as f:
            for line in f:
                fields = line.split(',')
                self.movie_names[fields[0]] = fields[1]

    def reducer_count_ratings(self, key, values):
        yield None, (sum(values), self.movie_names[key])

    # since key is none for all movies, this reducer will be called
    # once and run the max operation on the entire data set
    def reducer_getpopularmovie(self, key, values):
        yield max(values)

if __name__ == '__main__':
    MRPopularMovie.run()
