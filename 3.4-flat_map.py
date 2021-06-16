import apache_beam as beam

p1 = beam.Pipeline()

Collection = (
    p1
    |beam.io.ReadFromText('data/poema.txt')
    |beam.FlatMap(lambda record: record.split(' '))
    |beam.io.WriteToText('data/resultado3.4.txt')
)
p1.run()