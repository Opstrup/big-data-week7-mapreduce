from mrjob.job import MRJob


class MostVisitedCities(MRJob):

    def mapper(self, _, line):
        city = line.split(',')[0]
        for user in line.split(',')[1:]:
            yield int(user),city

    def reducer(self,user,cities):
        yield user,len(list(cities))


if __name__ == '__main__':
    MostVisitedCities.run()
# from mrjob.step import MRStep

# class MostVisitedPlaces(MRJob):

#     def mapper_seperating_data(self, _, line):
#         city = line.split(',')[0]
#         for user in line.split(',')[1:]:
#             yield (city, user), 1

#     def combiner_count_connections(self, city, values):
#         yield (city, sum(values))

#     def reduce_connections(self,city,value):
#     	if next(city) == city:
#     		yield(value,1)
#     	else:
#     		yield(value,0)	
#     # def reducer_check_for_euler(self, _, value):
#     #     if (1 != next(value)):
#     #         yield ("Not an euler", 1)
#     #     else:
#     #         yield ("An euler", 1)

#     # def combiner_check_not_euler_appereance(self, key, values):
#     #     yield (key, sum(values))

#     def steps(self):
#         return [
#             MRStep(mapper=self.mapper_seperating_data,
#                    combiner=self.combiner_count_connections)
#                    #reducer=self.reduce_connections
#                 #    reducer=self.reducer_check_for_euler
                  
#             # MRStep(reducer=self.combiner_check_not_euler_appereance)
#         ]

# if __name__ == '__main__':
#     MostVisitedPlaces.run()