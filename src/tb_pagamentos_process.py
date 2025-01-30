from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark

from datetime import datetime
def log():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' >>>'

print(log())

# Leitura dos dados
df_pagamentos = spark.read.csv("s3://lake-project-/0001_raw/tb_pagamentos/dados/", header=True)
df_pagamentos.createOrReplaceTempView('df_pagamentos')

qtd_registros = df_pagamentos.count()
print(log(),f'Quantidade de registros: {qtd_registros}')

print('Amostra tb_pagamentos original:')
df_pagamentos.show(5)

# Início das transformações
df_pagamentos_formated = spark.sql("""
SELECT
  REGEXP_REPLACE(id_pagamento, '.*-.*-', '') AS id_pagamento,
  REGEXP_REPLACE(id_fatura, '.*-', '') AS id_fatura,
  id_cliente,
  data_pagamento AS dt_pagamento,
  valor_pagamento AS vlr_pagamento
FROM
  df_pagamentos
""")
df_pagamentos_formated.createOrReplaceTempView('df_pagamentos_formated')

df_pagamentos_formated.printSchema()

dt_proc = datetime.now().strftime('%Y%m%d%H%M%S')
dt_proc

df_pagamentos_final = spark.sql(f"""
    SELECT
        CAST(id_pagamento AS BIGINT) AS id_pagamento,
        CAST(id_fatura AS BIGINT) AS id_fatura,
        CAST(id_cliente AS BIGINT) AS id_cliente,
        "{dt_proc}" AS dt_proc,
        SUBSTRING(REPLACE(dt_pagamento,'-',''),1,6) AS ref,
        TO_DATE(dt_pagamento) AS dt_pagamento,
        CAST(vlr_pagamento AS DECIMAL(14,2)) AS vlr_pagamento
    FROM
        df_pagamentos_formated
""")
df_pagamentos_final.createOrReplaceTempView('df_pagamentos_formated')

print('Amostra tb_pagamentos formatada incluindo ref e dt_proc:')
df_pagamentos_final.show(5)

# Salvamento dos dados transformados na camada trusted
df_pagamentos_final.write.partitionBy('ref').parquet("s3://lake-project-/0002_trusted/tb_0002_pagamentos", mode='append')

# Criação de um controle de processamento
print('Controle de tb_pagamentos:')
spark.sql(f"""
    SELECT
        'tb_0002_pagamentos' AS tb_name,
        '{dt_proc}' AS dt_proc,
        '{qtd_registros}' AS qtd_registros
""").show()

df_controle_pagamentos = spark.sql(f"""
    SELECT
        'tb_0002_pagamentos' AS tb_name,
        '{dt_proc}' AS dt_proc,
        '{qtd_registros}' AS qtd_registros
""")

df_controle_pagamentos.write.parquet(f"s3://lake-project-/0000_control/ctl_processed_filenames/trusted/tb_pagamentos_{dt_proc}", mode='append')