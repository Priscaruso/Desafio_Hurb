# Importa as bibliotecas a serem utilizadas no código
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import dataframe
import apache_beam.dataframe.io

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

# Permite realizar operações não paralelas com dataframes
with dataframe.allow_non_parallel_operations():
    # Cria um novo dataframe juntando o conteúdo dos dois beam dataframes criados, resetando o índice
    df = df_vendas_por_dia.reset_index(drop=True).join(df_estados_ibge.reset_index)

    # Renomeia a coluna 'UF [-]' do dataframe para 'Estado'
    df = df.rename(columns={'UF [-]': 'Estado'})

    # Remove todas as colunas não desejadas do dataframe
    df = df.drop(['Código [-]', 'Gentílico [-]', 'Governador [2019]', 'Capital [2010]',
                  'Área Territorial - km² [2019]', 'População estimada - pessoas [2020]',
                  'Densidade demográfica - hab/km² [2010]', 'Matrículas no ensino fundamental - matrículas [2018]',
                  'IDH <span>Índice de desenvolvimento humano</span> [2010]',
                  'Receitas realizadas - R$ (×1000) [2017]', 'Despesas empenhadas - R$ (×1000) [2017]',
                  'Rendimento mensal domiciliar per capita - R$ [2019]', 'Total de veículos - veículos [2018]'], axis=1)

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

    # Cria a coluna 'QtdVendas' com o valor de cada linha da tabela igual a 1, pois cada linha representa uma venda
    df['QtdVendas'] = 1

    # Cria coluna 'QtdAprovados' para todas as linhas que tenha valor de 'Status' Aprovado, substituindo seu valor
    # True para 1 e False para 0
    df['QtdAprovados'] = (df['Status'] == 'Aprovado')
    df['QtdAprovados'] = df['QtdAprovados'].replace({True: 1, False: 0})

    # Cria coluna 'QtdCancelamentos' para todas as linhas que tenha valor de 'Status' Cancelado, substituindo seu valor
    # True para 1 e False para 0
    df['QtdCancelamentos'] = (df['Status'] == 'Cancelado')
    df['QtdCancelamentos'] = df['QtdCancelamentos'].replace({True: 1, False: 0})

    # Agrupa os dados por 'Data', 'Estado' e 'UF', somando os valores númericos de 'QtdVendas', 'QtdAprovados',
    # 'QtdCancelamentos e armazenas essas alterações no dataframe df
    df = df.groupby(['Data', 'Estado', 'UF'], as_index=False)['QtdVendas', 'QtdAprovados', 'QtdCancelamentos'].\
        sum(numeric_only=True)

    # Converte os dados das colunas 'Estados' e 'UF' para string
    df['Estado'] = df['Estado'].convert_dtypes(convert_string=True)
    df['UF'] = df['UF'].convert_dtypes(convert_string=True)

    # Converte o dataframe df gerado para CSV sem salvar os índices, armazenando o arquivo na pasta output
    df.to_csv('/output/desafio_hurb.csv', index=False)

    # Converte o dataframe df gerado para JSON sem salvar os índices, com indentação de 4 linhas, armazenando o arquivo
    # na pasta output
    df.to_json('/output/desafio_hurb.json', orient='records', force_ascii=True, indent=4)

if __name__ == '__main__':
    pipeline.run()
