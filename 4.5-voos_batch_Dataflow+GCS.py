import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = {
    'project': 'project' ,
    'runner': 'DataflowRunner',
    'region': 'us-east1',
    'staging_location': 'gs://fundos-de-investimento/curso-apache-beam/temp',
    'temp_location': 'gs://fundos-de-investimento/curso-apache-beam/temp',
    'template_location': 'gs://fundos-de-investimento/curso-apache-beam/template/batch_job_df_gcs_voos' }
    
pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
p1 = beam.Pipeline(options=pipeline_options)

serviceAccount = '/home/rafaelols/keys/gcs_fi_sa.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= serviceAccount

class filtro(beam.DoFn):
  def process(self,record):
    if int(record[8]) > 0:
      return [record]

Tempo_Atrasos = (
  p1
  | "Importar Dados Atraso" >> beam.io.ReadFromText("gs://fundos-de-investimento/curso-apache-beam/entrada/voos_sample.csv", skip_header_lines = 1)
  | "Separar por Vírgulas Atraso" >> beam.Map(lambda record: record.split(','))
  | "Pegar voos com atraso" >> beam.ParDo(filtro())
  | "Criar par atraso" >> beam.Map(lambda record: (record[4],int(record[8])))
  | "Somar por key" >> beam.CombinePerKey(sum)
)

Qtd_Atrasos = (
  p1
  | "Importar Dados" >> beam.io.ReadFromText("gs://fundos-de-investimento/curso-apache-beam/entrada/voos_sample.csv", skip_header_lines = 1)
  | "Separar por Vírgulas Qtd" >> beam.Map(lambda record: record.split(','))
  | "Pegar voos com Qtd" >> beam.ParDo(filtro())
  | "Criar par Qtd" >> beam.Map(lambda record: (record[4],int(record[8])))
  | "Contar por key" >> beam.combiners.Count.PerKey()
)

tabela_atrasos = (
    {'Qtd_Atrasos':Qtd_Atrasos,'Tempo_Atrasos':Tempo_Atrasos} 
    | beam.CoGroupByKey()
    | beam.io.WriteToText("gs://fundos-de-investimento/curso-apache-beam/saida/Voos_atrados_qtd.csv")
)

p1.run()