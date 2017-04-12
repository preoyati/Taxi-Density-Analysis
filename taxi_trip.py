"""The classic MapReduce job: count the frequency of words.
"""
from mrjob.job import MRJob
from geopy.geocoders import Nominatim
from pyzipcode import ZipCodeDatabase
import re



class Cardensity(MRJob):

    def mapper(self, _, line):
	st = line.split(",")
        if st[10]!="pickup_longitude":
           geolocator = Nominatim()
           #print st[11]+","+st[10]
           try:
               location = geolocator.reverse(st[11]+","+st[10])
               #print location.address
               adds = location.address.split(",")
           
               for st in adds:
                   strin = re.sub(" ","",st)
                   #print strin + " len = " + str( len(strin)) + " cond = " + str(strin.isdigit())
                   if strin.isdigit() and len(strin)==5:
                      print strin
                      yield ( int(strin),1)
           except:
                  pass
                  
    def combiner(self, zipcode, counts):
        yield (zipcode, sum(counts))

    def reducer(self, zipcode, counts):
        zcdb = ZipCodeDatabase()
        try:
            locationinfo = zcdb[zipcode]
            yield ((locationinfo.latitude,locationinfo.longitude), sum(counts))
        except:
               pass
        

if __name__ == '__main__':
     Cardensity.run()

