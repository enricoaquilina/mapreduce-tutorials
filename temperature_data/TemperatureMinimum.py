from mrjob.job import MRJob

class MRTemperatureMinimum(MRJob):
    def make_celcius(self, tenthsOfCelcius):
        return float(tenthsOfCelcius) / 10

    def mapper(self, key, line):
        (weather_station, year, type, temperature, w, x, y, z) = line.split(',')
        if type == 'TMIN':
            temp = self.make_celcius(temperature)
            yield weather_station, temp

    def reducer(self, weather_station, temp):
        yield weather_station, min(temp)

if __name__ == '__main__':
    MRTemperatureMinimum.run()
