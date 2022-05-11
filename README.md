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

-Importação das bibliotecas: foram utilizadas as bibliotecas dataframe, 

-Configuração das opções do 

-Leitura de cada arquivos CSV: Para realizar a leitura dos arquivos CSV foi pensado na nova funcionalidade Beam Dataframe do Apache Beam, que já foi o módulo read_csv, facilitando na leitura desse tipo de arquivo.

-Merge dos arquivos CSV: junção com join resetando os índices de cada dataframe, para unir dataframes com múltiplos índices.

-Mudança do nome da coluna 'UF': a coluna 'UF [-]' foi renomeada para 'Estado' usando a função 'replace'.

-Filtragem e substituição dos valores da coluna 'Estado': os dados ficaram desorganizados com a junção, então foram organizados corretamente de acordo com os valores da coluna 'UF' usando a operação 'loc'

-Criação da coluna 


## Execução do Script


## Execução Notebook
