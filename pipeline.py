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

# Cria um novo dataframe juntando o conteúdo dos dois beam dataframes criados
merged_df = df_estados_ibge.join(df_vendas_por_dia, how='outer')

# Renomeia a coluna 'UF [-]' do dataframe para 'Estado'
renamed_column_df = merged_df.rename(columns={'UF [-]': 'Estado'})

# Remove todas as colunas não desejadas do dataframe
dropped_columns_df = renamed_column_df.drop(['Código [-]','Gentílico [-]','Governador [2019]','Capital [2010]',
                                             'Área Territorial - km² [2019]', 'População estimada - pessoas [2020]',
                                             'Densidade demográfica - hab/km² [2010]',
                                             'Matrículas no ensino fundamental - matrículas [2018]',
                                             'IDH <span>Índice de desenvolvimento humano</span> [2010]',
                                             'Receitas realizadas - R$ (×1000) [2017]',
                                             'Despesas empenhadas - R$ (×1000) [2017]',
                                             'Rendimento mensal domiciliar per capita - R$ [2019]',
                                             'Total de veículos - veículos [2018]'], axis=1)

# Simula o beam dataframe como um pandas dataframe para usar transformações em pandas
df = ib.collect(dropped_columns_df)

# Filtra e substitui os valores de NaN pelos estados de acordo com os valores da coluna 'UF'
filtro_AC = (df['UF'] == 'AC')
df.loc[filtro_AC, 'Estado'] = 'Acre'

filtro_AL = (df['UF'] == 'AL')
df.loc[filtro_AL, 'Estado'] = 'Alagoas'

filtro_AP = (df['UF'] == 'AP')
df.loc[filtro_AP, 'Estado'] = 'Amapá'

filtro_AM = (df['UF'] == 'AM')
df.loc[filtro_AM, 'Estado'] = 'Amazonas'

filtro_BA = (df['UF'] == 'BA')
df.loc[filtro_BA, 'Estado'] = 'Bahia'

filtro_CE = (df['UF'] == 'CE')
df.loc[filtro_CE, 'Estado'] = 'Ceará'

filtro_DF = (df['UF'] == 'DF')
df.loc[filtro_DF, 'Estado'] = 'Distrito Federal'

filtro_ES = (df['UF'] == 'ES')
df.loc[filtro_ES, 'Estado'] = 'Espírito Santo'

filtro_GO = (df['UF'] == 'GO')
df.loc[filtro_GO, 'Estado'] = 'Goiás'

filtro_MA = (df['UF'] == 'MA')
df.loc[filtro_MA, 'Estado'] = 'Maranhão'

filtro_MT = (df['UF'] == 'MT')
df.loc[filtro_MT, 'Estado'] = 'Mato Grosso'

filtro_MS = (df['UF'] == 'MS')
df.loc[filtro_MS, 'Estado'] = 'Mato Grosso do Sul'

filtro_MG = (df['UF'] == 'MG')
df.loc[filtro_MG, 'Estado'] = 'Minas Gerais'

filtro_PA = (df['UF'] == 'PA')
df.loc[filtro_PA, 'Estado'] = 'Pará'

filtro_PB = (df['UF'] == 'PB')
df.loc[filtro_PB, 'Estado'] = 'Paraíba'

filtro_PR = (df['UF'] == 'PR')
df.loc[filtro_PR, 'Estado'] = 'Paraná'

filtro_PE = (df['UF'] == 'PE')
df.loc[filtro_PE, 'Estado'] = 'Pernambuco'

filtro_PI = (df['UF'] == 'PI')
df.loc[filtro_PI, 'Estado'] = 'Piauí'

filtro_RJ = (df['UF'] == 'RJ')
df.loc[filtro_RJ, 'Estado'] = 'Rio de Janeiro'

filtro_RN = (df['UF'] == 'RN')
df.loc[filtro_RN, 'Estado'] = 'Rio Grande do Norte'

filtro_RS = (df['UF'] == 'RS')
df.loc[filtro_RS, 'Estado'] = 'Rio Grande do Sul'

filtro_RO = (df['UF'] == 'RO')
df.loc[filtro_RO, 'Estado'] = 'Rondônia'

filtro_RR = (df['UF'] == 'RR')
df.loc[filtro_RR, 'Estado'] = 'Roraima'

filtro_SC = (df['UF'] == 'SC')
df.loc[filtro_SC, 'Estado'] = 'Santa Catarina'

filtro_SP = (df['UF'] == 'SP')
df.loc[filtro_SP, 'Estado'] = 'São Paulo'

filtro_SE = (df['UF'] == 'SE')
df.loc[filtro_SE, 'Estado'] = 'Sergipe'

filtro_TO = (df['UF'] == 'TO')
df.loc[filtro_TO, 'Estado'] = 'Tocantins'







