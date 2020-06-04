from mrjob.job import MRJob
import time
import statistics
from mrjob.step import MRStep
import json
import io

class repl_stock_join(MRJob):


    sector_table = {}
    #
    def steps(self):
        return [MRStep(mapper_init=self.mapper_join_init,
                        mapper=self.mapper_repl_join),
                MRStep(mapper=self.mapper_length,
                        reducer=self.reducer_sum)]

    def mapper_join_init(self):
        # load companylist into a dictionary
        # run the job with --file input/companylist.tsv


        with open('scams.json', 'r') as f:
            file = json.load(f)
        for x in file['result']:

                key = x
                val = file['result'][x]['category']
                self.sector_table[key] = val


    def mapper_repl_join(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 7:
                address=fields[2]
                value=int(fields[3])
                time_epoch = fields[6]
                transac = time.strftime("%Y-%m",time.gmtime(int(time_epoch)))
                for x in self.sector_table:
                    #
                    if address == x:
                        category=self.sector_table[x]
                        yield((transac,category),value)
                        break

                    else:
                        pass

        except:
            pass

    def mapper_length(self,key,values):
        yield(key,values)

    def reducer_sum(self, key,values):
        yield(key,sum(values))
        # for x in values:
        #     yield(key,x) # we only would have 1 pair of values

if __name__ == '__main__':
    repl_stock_join.run()
