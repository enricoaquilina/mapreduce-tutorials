from mrjob.job import MRJob
from mrjob.step import MRStep

from math import sqrt
from itertools import combinations

class MovieRecommender(MRJob):
    def configure_options(self):
        super(MovieRecommender, self).configure_options()
        self.add_file_option('--items', help='Path to movies.csv')

    def load_names(self):
        self.movie_names = {}
        with open('movies.csv') as f:
            for line in f:
                fields = line.split(',')
                self.movie_names[fields[0]] = fields[1]

    def cosine_similarity(self, rating_pairs):
        # this will compute the similarity between the 2 vectors
        num_pairs = 0
        sum_xx = sum_yy = sum_xy = 0
        for rating_x, rating_y in rating_pairs:
            sum_xx += rating_x * rating_x
            sum_yy += rating_y * rating_y
            sum_xy += rating_x * rating_y
            num_pairs += 1

        numerator = sum_xy
        denominator = sqrt(sum_xx) * sqrt(sum_yy)

        score = 0
        if denominator:
            score = numerator / float(denominator)

        return score, num_pairs

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_user_movie_ratings,
                   reducer=self.reducer_movie_rating_pairs_by_user),
            MRStep(mapper=self.create_movie_pairs,
                   reducer=self.reducer_compute_similarity),
            MRStep(mapper=self.mapper_sort_movie_and_similarity,
                   mapper_init=self.load_names,
                   reducer=self.reducer_output_similarities),
        ]

    def mapper_get_user_movie_ratings(self, key, line):
        (user, movie_id, rating, timestamp) = line.split(',')
        yield user, (movie_id, float(rating))

    def reducer_movie_rating_pairs_by_user(self, user, movie_ratings):
        # mapreduce will sort and group by the key in the previous stage
        # i.e. the user, thus we run per each user's movie, rating pair
        ratings = []
        for movie, rating in movie_ratings:
            ratings.append((movie, rating))

        # this produces a user and a list of his movies and ratings
        yield user, ratings

    def create_movie_pairs(self, user, movie_ratings):
        for user_info_pair1, user_info_pair2 in combinations(movie_ratings, 2):
            movie1 = user_info_pair1[0]
            rating1 = user_info_pair1[1]
            movie2 = user_info_pair2[0]
            rating2 = user_info_pair2[1]

            yield (movie1, movie2), (rating1, rating2)
            yield (movie2, movie1), (rating2, rating1)

    def reducer_compute_similarity(self, movie_pair, rating_pair):
        score, num_pairs = self.cosine_similarity(rating_pair)

        if num_pairs > 50 and score > 0.95:
            yield movie_pair, (score, num_pairs)

    def mapper_sort_movie_and_similarity(self, movie_pair, info_pair):
        # just shuffling things and placing (movie1, score)
        movie1, movie2 = movie_pair
        score, n = info_pair

        yield (self.movie_names[movie1], score), (self.movie_names[movie2], n)

    def reducer_output_similarities(self, movie_score, similar_occurrences):
        movie1, score = movie_score
        # n is basically the confidence in the similarity rating 'score'
        for movie2, n in similar_occurrences:
            yield movie1, (movie2, score, n)


if __name__ == '__main__':
    MovieRecommender.run()
