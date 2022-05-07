# Importa as bibliotecas a serem utilizadas no código
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.runners.interactive.interactive_beam as ib
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
import csv

# Configura as opções usadas na execução do pipeline
    #beam_options = PipelineOptions(
    #runner='DirectRunner',
    #project='data-pipeline',
    #job_name='unique-job-name',
    #temp_location='/home/priscila/PycharmProjects/Projeto_apache_beam',
)

# Abre os arquivos, lê cada linha, transforma cada uma no formato de dicionário e armazena os dados em variáveis
with open('input/EstadosIBGE.csv') as f:
    estados_ibge = [dict(row) for row in csv.DictReader(f)]

with open('input/Vendas_por_dia.csv') as f:
    vendas_por_dia = [dict(row) for row in csv.DictReader(f)]

with beam.Pipeline(InteractiveRunner()) as pipeline:
    pcoll_estados_ibge = (pipeline | 'Cria a primeira PCollection' >> beam.Create(estados_ibge))
    pcoll_vendas_por_dia = (pipeline | 'Cria a segunda PCollection' >> beam.Create(vendas_por_dia))