"""An example Beam pipeline for Big Query to csv on Google Cloud"""

 import apache_beam as Beam

 """Function to transform rowdict object

 Args: rowdict object is passed into the Function transform

 returns :the result which is a csv via the yield statement"""

 def transform(rowdict):
     import copy
     result  = copy.deepcopy(rowdict)
     if rowdict['a'] > 0:
         result['c'] = result['a'] * result['b'] #applies data transformation
         yield ','.join([str(result[k]) if k in result else 'None' for k in ['a', 'b', 'c']])

"""create a beam pipeline"""


if __name__ == '__main__':
    p = beam.Pipeline(argv = sys.argv)
    selquery = 'SELECT a,b FROM someds.sometable'
    (p
            |beam.io.Read(beam.io.BigQuerySource(query = selquery,
                             use_standard_sql =True))
            |beam.Map(transform_data) # do some processing with the Map method)
            |beam.io.WriteToText('gs://...') #write output
            )

    p.run() #execcute and run the pipeline
