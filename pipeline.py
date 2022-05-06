# Importa as bibliotecas a serem utilizadas no código
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

beam_options = PipelineOptions(
    runner='DirectRunner',
    project='data-pipeline',
    job_name='unique-job-name',
    temp_location='/home/priscila/PycharmProjects/Projeto_apache_beam',
)

with beam.Pipeline(options=beam_options) as pipeline:
    read_first_file = (
        pipeline
        | beam.io.ReadFromText('/home/priscila/PycharmProjects/Projeto_apache_beam'
                                                                '/input/*.csv', skip_header_lines=True)
        | beam.Map(lambda x: x.split(','))
        | beam.Map(print)
    )