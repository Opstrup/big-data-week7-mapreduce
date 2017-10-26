from mrjob.job import MRJob
from mrjob.step import MRStep

class EulerChecker(MRJob):

    def mapper_node_connections(self, _, edge):
        yield (edge.split()[0]), 1
        yield (edge.split()[1]), 1

    def reducer_sum_connections(self, _, counts):
        yield None, sum(counts)

    def reducer_check_for_euler(self, _, degrees):
        has_euler_tour = True
        for degree in degrees:
            has_euler_tour = has_euler_tour & (degree % 2 == 0) 
        yield "Has euler tour", has_euler_tour

    def steps(self):
        return [
            MRStep(mapper=self.mapper_node_connections,
                   reducer=self.reducer_sum_connections
                   ),
            MRStep(reducer=self.reducer_check_for_euler)
        ]

if __name__ == '__main__':
    EulerChecker.run()