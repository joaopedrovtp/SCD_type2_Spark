## Importando pacotes 
# -*- coding: utf-8 -*-
import sys
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from os.path import abspath
from pyspark.sql.functions import udf, when, date_sub, lit
from pyspark.sql.types import ArrayType, IntegerType, StructType, StructField, StringType, BooleanType, DateType, FloatType, DoubleType
from pyspark.sql import Row
from datetime import datetime, timedelta
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
# hide info logs
quiet_logs(SparkContext())

if __name__ == "__main__":

    # Local do Warehouse default para armazenamento de banco de dados e tabelas
    warehouse_location = abspath('/user/hive/warehouse')

    # Configuração para acessar e manipular tabelas no hive
    spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

    # Configuração do SparkSQL Context para manipulação
    sc = SparkContext.getOrCreate()
    sqlContext = SQLContext(sc)

    # Coletando em um dataframe a tabela 'location' do db 'adworks' 
    df_clientes = spark.sql("select * from companydb.clientes")

    print("\n"+"Tabela 'clientes' atual no banco de dados:")
    df_clientes.show()

    # Criando um dataframe para usar como dados de entrada
    # Aqui provavelmente seria carregado de um DB ou csv. 
    dados_entrada = [
        Row(1001,'Joana','Casada','joana@email.com'),
        Row(1002, 'João', 'Casado', 'newjoao@email.com'),
        Row(1004, "Gabriela",'Casada', 'newgabriela@email.com'),
    ]

    schema_entrada = StructType([StructField("ent_ClienteID", IntegerType(), True),
        StructField("ent_nome", StringType(), True),
        StructField("ent_estado_civil", StringType(), True),
        StructField("ent_email", StringType(), True)]
    )

    df_entrada = sqlContext.createDataFrame(
        sc.parallelize(dados_entrada),
        schema_entrada
    )

    print("Dados de entrada:")
    df_entrada.show()

    ## Criando configuração SCD type 2 para os novos dados
    # Declarando datas de registro 'inicial' e 'final'
    data_final_ent = datetime.strptime('9999-12-31', '%Y-%m-%d').date()
    data_inicial_ent = datetime.today().date()
    ent_situation = 'ativo'

    # Concatenando as datas criadas com os dados da tabela de entrada
    df_entrada_new = df_entrada.withColumn('data_inicial_ent', lit(
    data_inicial_ent)).withColumn('data_final_ent', lit(data_final_ent)).withColumn('ent_situation', lit(ent_situation))
    
    # Realizando um fullouter joint para as tabelas, com critério do ID     
    df_merge = df_clientes.join(df_entrada_new, (df_entrada_new.ent_ClienteID == df_clientes.clienteid), how='fullouter')

    # Criando coluna de 'ação' para cada caso: Update, Delete, Insert e sem ação.
    df_merge = df_merge.withColumn('action',
    when((df_merge.ent_nome != df_merge.nome) | (df_merge.ent_estado_civil != df_merge.estado_civil) \
        | (df_merge.ent_email != df_merge.email) , 'UPSERT')
    .when(df_merge.ent_ClienteID.isNull(), 'DELETE')
    .when(df_merge.clienteid.isNull(), 'INSERT')
    .otherwise('NOACTION')
    )

    ## Criando novos dataframes pelas açoes definidas
    
    # Nome de colunas para formar os dataframes
    column_names = ['clienteid', 'nome', 'estado_civil',
    'email', 'start_date', 'final_date', 'situation']

    # Para casos 'sem açao'
    df_merge_p1 = df_merge.filter(
        df_merge.action == 'NOACTION').select(column_names)

    # Para casos de inserção
    df_merge_p2 = df_merge.filter(df_merge.action == 'INSERT').select(df_merge.ent_ClienteID.alias('clienteid'),
        df_merge.ent_nome.alias('nome'),
        df_merge.ent_estado_civil.alias('estado_civil'),
        df_merge.ent_email.alias('email'),
        df_merge.data_inicial_ent.alias('start_date'),
        df_merge.data_final_ent.alias('final_date'),
        df_merge.ent_situation.alias('situation')
        )

    # Para os casos deletados
    df_merge_p3 = df_merge.filter(
        df_merge.action == 'DELETE').select(column_names).withColumn('final_date', lit(datetime.today().date())) \
            .withColumn('situation', lit('expirado'))

    # Para casos de update 
    df_merge_p4_1 = df_merge.filter(
        df_merge.action == 'UPSERT').select(column_names).withColumn('final_date', lit(datetime.today().date()-timedelta(1))) \
            .withColumn('situation', lit('expirado'))
    df_merge_p4_2 = df_merge.filter(df_merge.action == 'UPSERT').select(df_merge.ent_ClienteID.alias('clienteid'),
        df_merge.ent_nome.alias('nome'),
        df_merge.ent_estado_civil.alias('estado_civil'),
        df_merge.ent_email.alias('email'),
        df_merge.data_inicial_ent.alias('start_date'),
        df_merge.data_final_ent.alias('final_date'),
        df_merge.ent_situation.alias('situation')
        )

    # Unir todos os dataframes criados 
    df_merge_final = df_merge_p1.unionAll(df_merge_p2).unionAll(df_merge_p3).unionAll(df_merge_p4_1).unionAll(df_merge_p4_2)
    df_merge_final.orderBy(['clienteid', 'start_date'])

    # Resultado final
    print("Tabela resultado:")
    df_merge_final.show()

    # Atualizar tabela no Hive 
    spark.sql(df_merge_final.write.mode("Overwrite").insertInto("companydb.clientes"))
