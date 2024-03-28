from mrjob.job import MRJob

class TagsParUtilisateur(MRJob):
    def mapper(self, _, line):
        try:
            (userID, movieID, rating, timestamp) = line.split('\t')
            yield userID, 1
        except Exception:
            pass

    def reducer(self, userID, occurrences):
        yield userID, sum(occurrences)

if __name__ == '__main__':
    TagsParUtilisateur.run()