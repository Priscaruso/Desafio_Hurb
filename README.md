# Desafio_Hurb

### Tópicos 

:small_blue_diamond: [Descrição do projeto](#descrição-do-projeto)

:small_blue_diamond: [Desenvolvimento do Script](#desenvolvimento-do-script)

:small_blue_diamond: [Execução do script](#execucao-do-script)

:small_blue_diamond: [Execução do notebook](#execucao-do-notebook)


## Descrição do projeto 

<p align="justify">
  O projeto consiste em criar um pipeline utilizando o Apache Beam que leia dois arquivos CSV e gere como saída dois arquivos: um em CSV e outro em JSON.
  Os arquivos de saída do pipeline devem ter as seguintes colunas:
  -Data
  -Estado
  -UF
  -QtdVendas
  -QtdAprovados
  -QtdCancelamentos
  
  As razões de se utilizar o Apache Beam para criação de pipelines, é que ele permite criar um modelo de programação unificada tanto para processamento em batch quanto
  para streaming. Isso simplica bastante o processamento em larga escala. Além disso, ele permite a utilização de diversas linguagens de programação; é open-source, o
  que facilita seu uso em diversos projetos; pode ser utilizado por diversos ambientes de processamento de dados distribuídos, como Apache Spark e Google Cloud Dataflow
  e executa tarefas ETL, muito usada para mover dados entre diferentes fontes de armazenamento.
</p>

## Desenvolvimento do Script
Para o desenvolvimento do script, foi pensado na divisão do mesmo de acordo com as seguintes etapas:

-Importação das bibliotecas: foram utilizadas as bibliotecas apache_beam, módulo dataframe da biblioteca apache_beam para operar com dataframes, módulo PipelineOptions do apache_beam.options.pipeline_options para configurar as opções do pipeline e o módulo apache_beam.dataframe.io para usar o módulo read_csv na leitura de arquivos csv.

-Configuração das opções do pipeline: uso do módulo beam.options para configurar o Runner que executará o pipeline, que no caso é o DirectRunner para ser executado localmente.

-Criação do objeto pipeline: objeto que é utilizado na criação do pipeline, usando as configurações feitas nas opções no passo anterior.

-Leitura de cada arquivos CSV: Para realizar a leitura dos arquivos CSV foi pensado na nova funcionalidade Beam Dataframe do Apache Beam, que já foi o módulo read_csv, facilitando na leitura desse tipo de arquivo.

-Criação de bloco para execução de operações não paralelas com dataframes: usa a função dataframe.allow_non_parallel_operations() para poder realizar operações não suportadas com o Beam Dataframe. Todas as próximas etapas são executadas dentro desse bloco.

-Merge dos arquivos CSV: junção com join resetando os índices de cada dataframe, para unir dataframes com múltiplos índices.

-Mudança do nome da coluna 'UF': a coluna 'UF [-]' foi renomeada para 'Estado' usando a função 'replace'.

-Remoção de todas as colunas não desejadas: uso da função drop para remover as colunas não desejadas 

-Filtragem e substituição dos valores da coluna 'Estado': os dados ficaram desorganizados com a junção, então foram organizados corretamente de acordo com os valores da coluna 'UF' usando a operação 'loc'

-Criação da coluna 'QtdVendas': a coluna é criada atribuindo-se valor igual a 1 a cada linha dessa coluna usando a operação df['QtdVendas'] = 1. Sabe-se que cada linha 
do dataframe representa uma venda, por isso valor igual a 1.

-Criação da coluna 'QtdAprovados': a coluna é criada para todas as linhas que tenha o valor da coluna 'Status' igual 'Aprovado'. Como o valor gerado é booleano, usa-se o replace para substituir o valor de 'True' para '1' e 'False' para '0'.

-Criação da coluna 'QtdCancelamentos': a coluna é criada para todas as linhas que tenha o valor da coluna 'Status' igual 'Cancelado'. Como o valor gerado é booleano, usa-se a função replace para substituir o valor de 'True' para '1' e 'False' para '0'.

-Agrupamento dos dados: agrupa-se os dados por 'Data', 'Estado' e ' UF' através da função groupby e soma os valores das linhas das colunas 'QtdVendas', 'QtdAprovados' e 'QtdCancelamentos' por meio da função sum.

-Conversão do formato dos dados para string: usa-se a função convert_dtypes com argumento convert_string=True para converter dados no formato objeto gerados no passo anterior nas colunas 'Estado' e 'UF'.

-Geração do arquivo CSV: uso do módulo to_csv para converter os dataframes para o formato de arquivo CSV, sem salvar os índices e especificando o local onde vai ser salvo o arquivo.

-Geração do arquivo JSON: uso do módulo to_json para converter os dataframes para o formato de arquivo JSON, sem salvar os índices e especificando o local onde vai ser salvo o arquivo, na forma de coluna: valor, forçando o uso do padrão ASCII e indentação igual a 4.

## Execução do Script
Os seguintes passos são necessários para executar o script no terminal:
-Baixar o script pipeline.py e a pasta input onde estão os arquivos de entrada para o pipeline
-Criar um ambiente virtual com os comandos 'python -m venv /path/to/directory' onde /path/to/directory é o diretório onde será criado o ambiente virtual
-Ativar o ambiente virtual com o comando '. /path/to/directory/bin/activate'
-Instalar o Apache Beam com o comando 'pip install apache-beam'
-Instalar a funcionalidade dataframe com o comando 'pip install apache-beam[dataframe]'
-Executar o pipeline localmente com o DirectRunner por meio do comando 'python -m apache_beam.pipeline --input /path/to/inputfile --output /path/to/write/counts', onde
/path/to/inputfile é o caminho para a pasta onde estão os arquivos de entrada baixado e '/path/to/write/counts' é o caminho para a pasta onde será armazenado os arquivos de saída gerados pelo pipeline


## Desenvolvimento do Notebook


## Execução do Notebook
