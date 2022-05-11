# Importa as bibliotecas a serem utilizadas no código
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.dataframe.io
import apache_beam.runners.interactive.interactive_beam as ib


# Configura as opções usadas na execução do pipeline
beam_options = PipelineOptions(
    runner='DirectRunner',
    project='data-pipeline',
    job_name='unique-job-name',
    temp_location='/home/priscila/PycharmProjects/Projeto_apache_beam')

# Cria o objeto pipeline com as configurações desejadas
pipeline = beam.Pipeline(options=beam_options)

# Pipeline que lê cada arquivo CSV como um Beam Dataframe
df_estados_ibge = (
        pipeline
        | 'Lê o primeiro arquivo CSV' >> beam.dataframe.io.read_csv('input/EstadosIBGE.csv')
    )

df_vendas_por_dia = (
        pipeline
        | 'Lê o segundo arquivo CSV' >> beam.dataframe.io.read_csv('input/Vendas_por_dia.csv')
    )
