from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark

from datetime import datetime
def log():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' >>>'

print(log())

# Leitura dos dados
df_fatura = spark.read.csv("s3://lake-project-/0001_raw/tb_faturas/dados/", header=True)
df_fatura.createOrReplaceTempView('df_fatura')

qtd_registros = df_fatura.count()
print(log(),f'Quantidade de registros: {qtd_registros}')

print('Amostra tb_faturas original:')
df_fatura.show(5)

# Incício das transformações
df_fatura_formated = spark.sql("""
SELECT
  REGEXP_REPLACE(id_fatura, '.*-', '') as id_fatura,
  id_cliente,
  data_emissao AS dt_emissao,
  data_vencimento AS dt_vencimento,
  valor_fatura AS vlr_fatura,
  valor_pagamento_minimo AS vlr_pagto_min
FROM
  df_fatura
""")
df_fatura_formated.createOrReplaceTempView('df_fatura_formated')

df_fatura_formated.printSchema()

dt_proc = datetime.now().strftime('%Y%m%d%H%M%S')
dt_proc

df_fatura_final = spark.sql(f"""
    SELECT
        CAST(id_fatura AS BIGINT) AS id_fatura,
        CAST(id_cliente AS BIGINT) AS id_cliente,
        "{dt_proc}" AS dt_proc,
        SUBSTRING(REPLACE(dt_emissao,'-',''),1,6) AS ref,
        TO_DATE(dt_emissao) AS dt_emissao,
        TO_DATE(dt_vencimento) AS dt_vencimento,
        CAST(vlr_fatura AS DECIMAL(14,2)) AS vlr_fatura,
        CAST(vlr_pagto_min AS DECIMAL(14,2)) AS vlr_pagto_min
    FROM
        df_fatura_formated
""")
df_fatura_final.createOrReplaceTempView('df_fatura_final')

print('Amostra tb_faturas formatada incluindo ref e dt_proc:')
df_fatura_final.show(5)

df_fatura_final.printSchema()

spark.sql("""
    SELECT
        ref
    FROM
        df_fatura_final
    GROUP BY
        ref
    ORDER BY
        ref
""").show()

# Salvamento dos dados transformados na camada trusted
df_fatura_final.write.partitionBy('ref').parquet("s3://lake-project-/0002_trusted/tb_0001_fatura", mode='append')

# Criação de um controle de processamento
print('Controle de tb_faturas:')
df_controle_fatura = spark.sql(f"""
    SELECT
        'tb_0001_fatura' AS tb_name,
        '{dt_proc}' AS dt_proc,
        '{qtd_registros}' AS qtd_registros
""").show()

df_controle_fatura = spark.sql(f"""
    SELECT
        'tb_0001_fatura' AS tb_name,
        '{dt_proc}' AS dt_proc,
        '{qtd_registros}' AS qtd_registros
""")

df_controle_fatura.write.parquet(f"s3://lake-project-/0000_control/ctl_processed_filenames/trusted/tb_faturas_{dt_proc}", mode='append')