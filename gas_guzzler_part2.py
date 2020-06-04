from mrjob.job import MRJob
import time
import statistics
from mrjob.step import MRStep


#This line declares the class Lab1, that extends the MRJob format.

class repartition_stock_join(MRJob):




    def mapper(self, _, line):


        if(len(line.split('\t')))==2:

            fields=line.split('\t')

            x=fields[1].replace('[','')
            z=x.replace(']','')
            y=z.split(',')
            gas=y[0].replace('"','')
            block=y[1].replace('"','')
            time=y[2][2:9]
            yield (time,(gas,1))

    def combiner(self,month,gases):

        count1=0
        count2=0
        for x in gases:
            count1+=int(x[0])
            count2+=int(x[1])

        # print("total words appearing more than 10 times = "+count(word))
        yield(month,(count1,count2))

    def reducer(self, month, sums):

        count1=0
        count2=0
        for x in sums:
            count1+=int(x[0])
            count2+=int(x[1])

        average=count1/count2

        # print("total words appearing more than 10 times = "+count(word))
        yield(month,average)


if __name__ == '__main__':
    repartition_stock_join.run()
