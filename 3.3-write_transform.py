import apache_beam as beam

p1 = beam.Pipeline()

voos = (
p1
  | "Importar Dados" >> beam.io.ReadFromText("data/voos_sample.csv", skip_header_lines = 1)
  | "Separar por Vírgulas" >> beam.Map(lambda record: record.split(','))
  | "Gravar Resultado" >> beam.io.WriteToText("data/voos.txt")
)

p1.run()
