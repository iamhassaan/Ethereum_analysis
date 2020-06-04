from mrjob.job import MRJob
import time
import statistics
from mrjob.step import MRStep


#This line declares the class Lab1, that extends the MRJob format.





class repl_stock_join(MRJob):


    sector_table = {}

    def steps(self):
        return [MRStep(mapper_init=self.mapper_join_init,
                        mapper=self.mapper_repl_join),
                MRStep(mapper=self.mapper_length,
                        reducer=self.reducer_sum)]


    def mapper_join_init(self):
        # load companylist into a dictionary
        # run the job with --file input/companylist.tsv

        with open("partb.txt") as f:
            for line in f:
                fields = line.split("-")
                key = fields[0]
                val = fields[1]
                self.sector_table[key] = val
    # def mapper_repl_join(self, _, line):
    def mapper_repl_join(self, _, line):
        fields = line.split(",")
        address=fields[2]
        date=fields[6]
        gas=fields[4]
        value=fields[3]


        for x in self.sector_table:

            if address == x:
                tiime=time.strftime("%Y-%m-%d",time.gmtime(int(date)))
                pair=(tiime,gas,value)
                yield(address,pair)
                break



    def mapper_length(self,key,values):
        yield(key,list(values))

    def reducer_sum(self, key,values):
        for x in values:
            yield(key,x)

if __name__ == '__main__':
    repl_stock_join.run()
