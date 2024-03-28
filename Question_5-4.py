from mrjob.job import MRJob

class TagsFilmParUtilisateur(MRJob):
    def mapper(self, _, line):
        try:
            parts = line.split(',')
            userID = parts[0]
            movieID = parts[1]
            yield (userID, movieID), 1
        except Exception:
            pass

    def reducer(self, user_movie, occurrences):
        yield user_movie, sum(occurrences)

if __name__ == '__main__':
    TagsFilmParUtilisateur.run()