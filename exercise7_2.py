from mrjob.job import MRJob
from mrjob.step import MRStep

class EulerChecker(MRJob):

    def mapper_node_connections(self, _, line):
        yield ((line[0], line[2]), 1)

    def combiner_count_connections(self, connection, counts):
        yield (connection, sum(counts))

    def reducer_check_for_euler(self, _, value):
        if (all(v == 1 for v in value)):
            yield None, "Euler!"
        else:
            yield None, "Not an Euler!"

    def reducer_(self, _, value):
        if (all(v == 1 for v in value)):
            yield None, "Euler!"
        else:
            yield None, "Not an Euler!"

    def steps(self):
        return [
            MRStep(mapper=self.mapper_node_connections,
                   combiner=self.combiner_count_connections,
                   reducer=self.reducer_check_for_euler
                   ),
            #MRStep(reducer=self.reducer_check_for_euler)
        ]

if __name__ == '__main__':
    EulerChecker.run()