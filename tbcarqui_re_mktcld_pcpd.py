# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from importlib import import_module
from datetime import datetime
from os import listdir
from os.path import isfile, join
import subprocess
from functools import reduce
import sys

if __name__ == "__main__":

    spark = SparkSession.builder.config('hive.exec.dynamic.partition.mode', 'nonstrict').getOrCreate()
    spark.conf.set('hive.exec.dynamic.partition', 'true')
    spark.conf.set('hive.merge.tezfiles', 'true')
    spark.conf.set('hive.merge.smallfiles.avgsize', '128000000')
    spark.conf.set('hive.merge.size.per.task', '128000000')
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    spark.conf.set('spark.sql.files.ignoreMissingFiles', 'true')
    spark.conf.set('spark.sql.parquet.binaryAsString', 'true')

    bucket_t = spark.conf.get('spark.yarn.appMasterEnv.TRUSTED_BUCKET')
    bucket_r = spark.conf.get('spark.yarn.appMasterEnv.REFINED_BUCKET')

    df_ec = spark.read.parquet('s3://' + bucket_t +'/sie/tbdwr_ec')
    #df_ec = spark.sql("select * from sie.tbdwr_ec")
    
    df_cad = df_ec.select(col("nu_ec"), col("nu_so_ec"), col("cd_so_cadeia_forcada"), col("cd_situ_ativ_ec"), col("nu_so_cpf_cnpj").cast("bigint"), col("nu_cnpj"))
    df_cad = df_cad.filter(df_cad.cd_situ_ativ_ec != "6")
    #and nvl(nu_cnpj,99) not in (-2,0,1,2)
    df_cad = df_cad.withColumn("nu_so_cpf_cnpj", coalesce(df_cad.nu_so_cpf_cnpj, lit(99)))
    df_cad = df_cad.filter(~df_cad.nu_cnpj.isin([-2,0,1,2]))
    df_cad = df_cad.dropDuplicates()


    df_ftrm_cond_diro = spark.read.parquet('s3://' + bucket_t + '/sie/tbdwf_ftrm_cond_diro')
    #df_ftrm_cond_diro = spark.sql("select * from sie.tbdwf_ftrm_cond_diro")

    df_p = df_ftrm_cond_diro.select(col("nu_so_ec"), col("vl_dsct_ngco"), col("vl_dsct_prcl"), col("cd_dia"), col("dt_carga"))
    df_p = df_p.filter(col("nu_so_ec").isNotNull())
    df_p = df_p.groupBy("nu_so_ec", "vl_dsct_ngco", "vl_dsct_prcl").agg(max("cd_dia").alias("cd_dia"), max("dt_carga").alias("dt_carga"))
    df_p = df_p.dropDuplicates()
    df_p = df_p.withColumn("vl_algl_dsct_finl", 
        when((coalesce(col("vl_dsct_ngco"), lit(0)) >= coalesce(col("vl_dsct_prcl"), lit(0))) & ((col("vl_dsct_prcl").isNull())), coalesce(col("vl_dsct_ngco"), lit(0)))
        .when((coalesce(col("vl_dsct_prcl"), lit(0)) > coalesce(col("vl_dsct_ngco"), lit(0))) & ((col("vl_dsct_ngco").isNull())), coalesce(col("vl_dsct_prcl"), lit(0)))
        .when(coalesce(col("vl_dsct_ngco"), lit(0)) == 0, coalesce(col("vl_dsct_prcl"), lit(0)))
        .when(coalesce(col("vl_dsct_prcl"), lit(0)) == 0, coalesce(col("vl_dsct_ngco"), lit(0)))
        .when(coalesce(col("vl_dsct_ngco"), lit(0)) >= col("vl_dsct_prcl"), col("vl_dsct_prcl"))
        .otherwise(col("vl_dsct_ngco")))
    df_p = df_p.withColumn("vl_algl_dsct_finl", df_p.vl_algl_dsct_finl.cast("double"))

    df_pco_agrp = df_p.filter(col("nu_so_ec").isNotNull())
    df_pco_agrp = df_pco_agrp.groupBy("nu_so_ec").agg(max("cd_dia").alias("max_cd_dia"), max("dt_carga").alias("max_dt_carga"), max("vl_algl_dsct_finl").alias("vl_algl_dsct_finl"))
    df_pco_agrp = df_pco_agrp.withColumnRenamed("nu_so_ec", "nu_so_ec_agrp")
    df_pco_agrp = df_pco_agrp.dropDuplicates()

    df_pco = df_ftrm_cond_diro.filter(col("nu_so_ec").isNotNull())
    df_pco = df_pco.select(col("nu_so_ec").alias("nu_so_ec_pco"), col("dt_acte"), col("vl_meta"),
        col("vl_dsct_ngco"), col("vl_dsct_prcl"), col("vl_ftrm_diro").alias("vl_ftrm_rlzd"),
        col("vl_fltn").alias("vl_meta_fltn"), col("qt_eqpm"),
        col("pc_alcd"), col("pc_fltn").alias("pc_meta_fltn"),
        col("flag_isnc").alias("pcpd_flag_isnc"),
        col("dt_carga").alias("pcpd_dt_carga"), col("id_cvnc"), col("cd_mes"), col("cd_dia"))
    df_pco = df_pco.dropDuplicates()
    df_pco = df_pco.withColumn("cd_mes", concat(substring("cd_mes", 1, 4), substring("cd_mes", 5, 2)).cast("bigint"))
    

    df_final = df_cad.join(df_pco, df_cad.nu_so_ec == df_pco.nu_so_ec_pco, 'left')
    df_final = df_final.join(df_pco_agrp, ([df_final.nu_so_ec == df_pco_agrp.nu_so_ec_agrp, df_final.cd_dia == df_pco_agrp.max_cd_dia, df_final.pcpd_dt_carga == df_pco_agrp.max_dt_carga]), 'inner')

    df_final = df_final.withColumn("dt_crga", current_timestamp())
    df_final = df_final.withColumn("dt_crga", concat(substring(col("dt_crga"), 0, 4), substring(col("dt_crga"), 6, 2), substring(col("dt_crga"), 9, 2)).cast("bigint"))

    df_final =  df_final.withColumn("subscriberkey", lit(''))

    df_final = df_final.withColumn("vl_totl_algl", col("vl_algl_dsct_finl") * col("qt_eqpm"))

    df_final.select(col("subscriberkey"), col("nu_ec"), col("nu_so_ec"), col("cd_so_cadeia_forcada"), col("nu_so_cpf_cnpj"),
        col("dt_acte"), col("vl_meta"), col("vl_dsct_ngco"), col("vl_dsct_prcl"),
        col("vl_ftrm_rlzd"), col("vl_meta_fltn"), col("qt_eqpm"),
        col("vl_totl_algl").cast("double").alias("vl_totl_algl"), col("vl_algl_dsct_finl").cast("double").alias("vl_algl_dsct_finl"), col("pc_alcd"), col("pc_meta_fltn"),
        col("id_cvnc"),  col("cd_mes"), col("dt_crga"))
    
    df_final = df_final.dropDuplicates()
    
    df_final.write.option("encoding", "UTF-8").mode('overwrite').parquet("s3://" + bucket_r + "/re_crm/tbcarqui_re_mktcld_pcpd")
