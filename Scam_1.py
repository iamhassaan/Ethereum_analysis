from mrjob.job import MRJob
import time
import statistics
from mrjob.step import MRStep
import json
# with open('scams.json', 'r') as f:
#     file = json.load(f)
#
# for x in file['result']:
    # print("{} address has a category of {}".format(x,file['result'][x]['category']))
#******************* TEST******************************
# sector_table={}
# with open('scams.json', 'r') as f:
#     file = json.load(f)
# for x in file['result']:
#
#     key = x
#     val = file['result'][x]['category']
#     sector_table[key] = val
# print(sector_table)
# #******************* TEST******************************


class repl_stock_join(MRJob):


    sector_table = {}
    #
    # def steps(self):
    #     return [MRStep(mapper_init=self.mapper_join_init,
    #                     mapper=self.mapper_repl_join),
    #             MRStep(mapper=self.mapper_length,
    #                     reducer=self.reducer_sum)]
    def steps(self):
        return [MRStep(mapper_init=self.mapper_join_init,
                        mapper=self.mapper_repl_join),
                MRStep(reducer=self.reducer_sum)]

    def mapper_join_init(self):
        # load companylist into a dictionary
        # run the job with --file input/companylist.tsv
        with open('scams.json', 'r') as f:
            file = json.load(f)


        for x in file['result']:

                key = x
                val = file['result'][x]['category']
                self.sector_table[key] = val
    # def mapper_repl_join(self, _, line):
    def mapper_repl_join(self, _, line):
        fields = line.split("\t")
        address=fields[0].replace('"','')
        value=int(fields[1])

        for x in self.sector_table:

            if address == x:

                category=self.sector_table[x]
                yield(category,value)
                break


    #
    # def mapper_length(self,key,values):
    #     yield(key,values)

    def reducer_sum(self, key,values):
        # for x in values:
        yield(key,sum(values)) # we only would have 1 pair of values

if __name__ == '__main__':
    repl_stock_join.run()
