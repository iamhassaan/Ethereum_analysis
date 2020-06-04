from mrjob.job import MRJob
import time
import statistics
from mrjob.step import MRStep


#This line declares the class Lab1, that extends the MRJob format.

class repartition_stock_join(MRJob):

    def steps(self):
        return [MRStep(mapper=self.mapper_join_init,
                        reducer=self.reducer_transactions),
                MRStep(mapper=self.mapper_new,
                        reducer=self.reducer_new)]



    def mapper_join_init(self, _, line):
        try:

            #one mapper, we need to first differentiate among both types
            if(len(line.split('\t')))==2:

                fields = line.split('\t')
                #this should be a company sector line
                join_key = fields[0].replace('"','')
                join_value = int(fields[1])
                yield (join_key, (join_value,1))

            elif(len(line.split(',')))==5:
                fields = line.split(',')
                #this should be a base closing price line
                join_key = fields[0]
                join_value = int(fields[3])
                yield (join_key,(join_value,2))

        except:
            pass

    def reducer_transactions(self, to_address, values):

        vals=0
        vars=False

        for value in values:
            if value[1] == 1:
                vals=value[0]
            elif value[1]==2:
                vars=True

        if vars==True and vals>0:
            x=(to_address,vals)
            yield(None,x)

    def mapper_new(self, _,x):
        yield(None,x)

    def reducer_new(self, _,vals):
        sorted_values = sorted(vals, reverse=True, key = lambda x: x[1])
        for x in range(10):
            yield(x+1,'{}-{}'.format(sorted_values[x][0],sorted_values[x][1]))

if __name__ == '__main__':
    repartition_stock_join.run()
