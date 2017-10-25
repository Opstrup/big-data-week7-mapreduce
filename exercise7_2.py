from mrjob.job import MRJob
from mrjob.step import MRStep

class EulerChecker(MRJob):

    def mapper_node_connections(self, _, line):
        yield ((line[0], line[2]), 1)

    def combiner_count_connections(self, connection, counts):
        yield (connection, sum(counts))

    def reducer_check_for_euler(self, _, value):
        if (1 != next(value)):
            yield ("Not an euler", 1)
        else:
            yield ("An euler", 1)

    def combiner_check_not_euler_appereance(self, key, values):
        yield (key, sum(values))

    def steps(self):
        return [
            MRStep(mapper=self.mapper_node_connections,
                   combiner=self.combiner_count_connections,
                   reducer=self.reducer_check_for_euler
                   ),
            MRStep(reducer=self.combiner_check_not_euler_appereance)
        ]

if __name__ == '__main__':
    EulerChecker.run()