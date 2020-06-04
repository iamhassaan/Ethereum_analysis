
from mrjob.job import MRJob
import time

#this is a regular expression that finds all the words inside a String

#This line declares the class Lab1, that extends the MRJob format.
class Lab2(MRJob):

# this class will define two additional methods: the mapper method goes here
    def mapper(self, _, line):
        try:

            fields = line.split(",")
            if (len(fields)==7):
            #access the fields you want, assuming the format is correct now

                x=fields[2]
                y=int(fields[3]) #value

                yield(x,y)

        except:
            pass


    def combiner(self,address,counts):

        yield(address,sum(counts))


    def reducer(self,address,counts):


        yield(address,sum(counts))


if __name__ == '__main__':

    Lab2.run()
