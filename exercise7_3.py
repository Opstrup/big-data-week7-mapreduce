from mrjob.job import MRJob


class MostVisitedCities(MRJob):

    def mapper(self, _, line):
        city = line.split(',')[0]
        for user in line.split(',')[1:]:
            yield int(user),city

    def reducer(self,user,cities):
        yield "ID: "+str(user),"has visited: "+str(len(list(cities)))+" cities"


if __name__ == '__main__':
    MostVisitedCities.run()
