#  Implementando Slow Changing Dimensions em um DW Usando Hive e Spark

Um dos mais amplos usos do Hadoop atualmente é construir uma plataforma de Data Warehousing sobre um Data Lake, através do Apache Hive.

Slowly Changing Dimension(SCD) é um termo utilizado em teorias de Data Management e Data Warehousing para grupos de dados lógicos como informações de produtos, clientes, etc. que mudam lentamente ao longo do tempo. Nesse exemplo vou implementar o SCD tipo 2 em uma tabela no DW do Hive utilizando o Apache Spark.

| ClienteID | Nome | Estado_civil | Email | Start_date | Final_date | Situation |
| --- | --- | --- | --- | --- | --- | --- |
| 1001 | Joana | Casada | joana@email.com | 2010-01-03 | 9999-12-30 | ativo |
| 1002 | João | Solteiro | joao@email.com | 2015-04-19 | 9999-12-30 | ativo |
| 1003 | Ricardo | Solteiro | ricardo@email.com | 2012-09-30 | 9999-12-30 | ativo |
| 1004 | Gabriela | Solteira | gabriela@email.com | 2014-08-05 | 9999-12-30 | ativo |




