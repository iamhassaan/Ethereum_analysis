
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

                time_epoch = int(fields[6])
                gas=int(fields[5])
                date=time.gmtime(time_epoch)
                month=date.tm_mon
                year=date.tm_year
                x=(month,year)
                yield(x,(gas,1))

        except:
            pass

    #
    # def combiner(self,day,counts):
    # #you have to implement the body of this method. Python's sum() function will probably be useful
    #
    #     # print("total words appearing more than 10 times = "+count(word))
    #     yield(day,sum(counts))

    def reducer(self,month,gas):
        #you have to implement the body of this method. Python's sum() function will probably be useful
        count1=0
        count2=0
        for x in gas:
            count1+=int(x[0])
            count2+=int(x[1])
        average=count1/count2

        # print("total words appearing more than 10 times = "+count(word))
        yield(month,average)

#this part of the python script tells to actually run the defined MapReduce job. Note that Lab1 is the name of the class
if __name__ == '__main__':

    Lab2.run()
