import apache_beam as beam
import datetime, os

"""First step is data preprocessing with Apache Beam
   either batch or stream data"""




def to_csv(rowdict):
  import copy
  import hashlib
  no_ultrasound = copy.deepcopy(rowdict)
  w_ultrasound = copy.deepcopy(rowdict)

  CSV_COLUMNS  = 'weight_pounds, is_male, mother_age, plurality, gestation_weeks'.split(',')

  no_ultrasound['is_male'] = 'Unknown'
  if rowdict['plurality'] > 1:
    no_ultrasound['plurality'] = 'Multiple(2+)'
  else:
    no_ultrasound['plurality'] = 'Single(1)'

  #change the plurality column to strings
  w_ultrasound['plurality'] = ['Single(1)', 'Twins(2)', 'Triplets(3)', 'Quadruplets(4)', 'Quintuplets(5)'][rowdict['plurality'] - 1]

  for result in [no_ultrasound, w_ultrasound]:
    data = ','.join([str(result[k]) if k in result else 'None' for k in CSV_COLUMNS])
    key = hashlib.sha224(data).hexdigest() #hash the columns to form a key









def preprocess(in_test_mode):
    import shutil,os,subprocess
    job_name  = 'preprocess-babyweight-features' + '-' + datetime.datetime.now().strftime('%y%m%d-%H%M%S')

    if in_test_mode:
        print 'Launching local job ...hang on'
        OUTPUT_DIR  = './preproc' #write ouput csv to /preproc
        shutil.rmtree(OUTPUT_DIR, ignore_errors = True)
        os.makedir(OUTPUT_DIR)
    else:
        print'Launching DataFlow job {} ....hang on'.format(job_name)
        OUTPUT_DIR = 'gs://{0}/babyweight/preproc/'.format(BUCKET)
        try:
            subprocess.check_call('gsutil -m rm -r {}'.format(OUTPUT_DIR).split())
        except:
            pass
    options = {
    'staging_location': os.path.join(OUTPUT_DIR, 'tmp', 'staging'),
    'temp_location': os.path.join(OUTPUT_DIR, 'tmp'),
    'job_name': job_name,
    'project': PROJECT,
    'teardown_policy': 'TEARDOWN_ALWAYS',
    'no_save_main_session': True
    }
opts  = beam.pipeline.PipelineOptions(flags = [], **options)
if in in_test_mode:
    RUNNER = 'DirectRunner'
else:
    RUNNER = 'DataFlowRunner'

p = beam.Pipeline(RUNNER, options = opts)










"""Modifying the query to pull into the fields needed"""

query  = """SELECT * FROM (
SELECT weight_pounds,is_male, mother_age, plurality,
gestation_weeks, ABS(FARM_FINGERPRINT(CONCAT(CAST(YEAR AS STRING), CAST(month AS STRING)))) AS hashmonth
FROM
publicdata.samples.natality
WHERE year > 2000 )
AND weight_pounds > 0
AND mother_age > 0
AND plurality > 0
AND gestation_weeks > 0
AND month > 0"""







if in_test_mode:
    query  = query + 'LIMIT 100'

for step in ['train', 'eval']:
    if step == 'train':
        selquery = 'SELECT *FROM ({}) WHERE MOD(ABS(hashmonth), 4) < 3'.format(query)
    else:
        selquery = 'SELECT *FROM ({}) WHERE MOD(ABS(hashmonth), 4) = 3'.format(query)

        """RUNNING CLOUD DATAFLOW"""

    (p
    | '{}_read'.format(step) >> beam.io.Read(beam.io.BigQuerySource(query = selquery, use_standard_sql = True))
    | '{}_csv'.format(step) >> beam.FlatMap(to_csv)
    | '{}_out'.format(step) >> beam.io.Write(beam.io.WriteToText (os.path.join(OUTPUT_DIR, '{}.csv'.format(step))))
    )

    job = p.run # execute CloudDataflow
    if in_test_mode:
        job.wait_until_finish()
        print "Done!"



"""Using the preprocss function
pass in the argument (in_test_mode)
if True data preprocessing is executed locally on sample data
if False data preprocessing is scaled and executed Cloud DataFlow"""

preprocess(in_test_mode = True)
