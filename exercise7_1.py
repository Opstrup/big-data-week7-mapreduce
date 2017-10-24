from mrjob.job import MRJob
from mrjob.job import MRStep
import re

WORD = re.compile(r"[\w']+")

class wordCounter(MRJob):

    def steps(self):
        return [
                MRStep(mapper=self.mapper_get_words,
                       reducer=self.reducer_counted_words)
                ]

    def mapper_get_words(self, _, line):
        for word in WORD.findall(line):
            yield (word.lower(), 1)

    def reducer_counted_words(self, word, counts):
        yield (word, sum(counts))

if __name__ == '__main__':
    wordCounter.run()
