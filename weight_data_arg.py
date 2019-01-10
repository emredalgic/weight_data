#!/usr/bin/env python
# coding: utf-8

#### import module ####
from sql_connection import SqlConnection
import pandas as pd 
import numpy as np
import sys

conn = SqlConnection() # windows authentication --> Emeatristsql  /  SSIVRDATA default


def get_rimtable(var):
    rimdf=conn.sql_readtable(
        "select * from SSIVRDATA.[dbo].[rim_weight_table] where ProjectNo='{}'".format(var))

    return(rimdf)

def table_control(tablename,weightcolumn=None):
    if weightcolumn == None :
            ifexist = conn.sql_readtable(
                "SELECT count(distinct 1) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}' ".format(tablename))
    else :
        ifexist = conn.sql_readtable("SELECT count(1) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}' and COLUMN_NAME='{}' ".format(tablename, weightcolumn))
        if ifexist.values[0] == 1 :
            datatipkontrol = conn.sql_readtable(
            "SELECT DATA_TYPE  FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}' and COLUMN_NAME='{}'".format(tablename,weightcolumn))
            if datatipkontrol.values[0] != "['float']" :
                conn.sql_execution("ALTER TABLE {} ALTER COLUMN {} float".format(tablename, weightcolumn))
        # print("{} tablosundaki agirlik yazilacak olan [{}] kolonunun datatipi '{}'.Lütfen float yapınız!".format(tablename,weightcolumn,datatipkontrol.values[0]))
    return(ifexist.values[0])

def control_weight(table_name,uniqueid,weight_column,matrisdf):
    df = conn.sql_readtable("select * from {}".format(table_name))
    df = pd.melt(df , id_vars=uniqueid,var_name="ColumnName", value_name="ColumnValue")
    matrisdf = get_rimtable(matris)
    df = df.loc[df['ColumnName'].isin(matrisdf["ColumnName"]) | df['ColumnName'].isin([weight_column])]
    
    dframe = pd.merge( df, df.loc[df["ColumnName"] == weight_column]  , on=uniqueid, how="inner", right_index=True)
    dframe.drop(["ColumnName_y"],axis = 1,inplace = True) 
    dframe.rename(index=str, columns={"ColumnName_x": "ColumnName" , "ColumnValue_y" :"weight","ColumnValue_x":"ColumnValue"}, inplace=True)
    
    dframe["weight"] = dframe["weight"].astype(np.float) 
    dframe["ColumnValue"] = dframe["ColumnValue"].astype(float)
    
    result = dframe.groupby(["ColumnName","ColumnValue"])[("weight")].agg({np.sum})
    matrisdf["ColumnValue"]=matrisdf["ColumnValue"].astype(float)
    result = pd.merge( matrisdf,result, on=["ColumnName", "ColumnValue"], how="left")
    result["ratio"] = result["TargetValue"] / result["sum"]
    return(result)

def rim_weight(tablename,matrisdf,weight_column,matris,uniqueid):
    script1 = (""" update T1 SET {}=1 FROM {} t1 """.format(weight_column,tablename))
    conn.sql_execution(script1)
    itr = 1
    while True:
        for j in matrisdf["ColumnName"].unique():
            # script0 = ("""select round(TargetValue/convert(float,sow),6) success_ratio_weight,sow,count(*) agirliksiz_adet,columnName""")
            script1 = ("""update t1 set {} = t1.{} * (TargetValue/convert(float,sow))""".format(weight_column,weight_column))
            script2 = (
            """
            from {} t1
            inner join 
            (
            select {},sum(convert(float,{})) sow
            from {}  where {} is not null
            group by {}
            ) t2
            on t1.{}=t2.{}
            inner join 
            (
            select ColumnValue,TargetValue,columnName from rim_weight_table where ProjectNo='{}' and columnName ='{}'
            ) t3
            on t3.ColumnValue = t1.{}  
            where t1.{}  is not null
            """.format(tablename,j,weight_column,tablename,j,j,j,j,matris,j,j,j))

            conn.sql_execution(script1+script2)
            
            scriptefficency=(
            """
            declare
            @n int,	
            @weightsum float,
            @weightsumsquare float

            select @n = count(*) ,
            @weightsum = SUM(convert(float,{})) ,
            @weightsumsquare = sum(power(convert(float,{}),2)) * power(count (*),2) / power(SUM(convert(float,{})),2)
            from {}

            select 100 * power(@n,2) / (convert(float,@weightsumsquare) * @n ) Efficiency  
            """.format(weight_column,weight_column,weight_column,tablename))
            Efficiency = conn.sql_readtable(scriptefficency)
                    
            itr += 1

            kontr = control_weight(tablename,uniqueid,weight_column,matris)
            # print(kontr)
            print(round(kontr["ratio"].mean(), 5), round(kontr["ratio"].std(), 3))
        if round(kontr["ratio"].mean(),5) == float(1.0) :
            print("Efficiency :",Efficiency.values[0] , "\n iterasyon :",itr-1 )
            print(kontr[["ColumnName","ColumnValue","TargetValue","sum","ratio"]])
            print("Agirliklandirma islemi bitti...")
            break
        
        # Ağırlıklandırma oranının mean ve standart sapmasını hesaplayıp döngüyü bitiriyoruz...
         
        elif round(kontr["ratio"].std(), 3) <= float(0.002) and round(kontr["ratio"].mean(), 5) <= float(1.0002):
            print("Efficiency :",
                  Efficiency.values[0], "\n iterasyon :", itr-1)
            print("weight_std :", round(kontr["ratio"].std(), 3),
                  "weight_mean :", round(kontr["ratio"].mean(), 5))
            print(kontr[["ColumnName", "ColumnValue",
                         "TargetValue", "sum", "ratio"]])
            print("###############################")
            print("Agirliklandirma islemi bitti...")
            break
        
        elif itr > 30 :
            #print("Efficiency :",Efficiency.values[0] , "\n iterasyon :",itr-1 ,"sow_mean",Kontrol.weight.mean(),"sow_stdev",Kontrol.weight.std())
            print("Lutfen agirlik matrisinizi ve tablonuzu kontrol edin 30 iterasyon sonra bile basarili bir agirliklandirma yapilamadi. :(")
            break


def _weight_run(matris,tablename):

    rimdf = get_rimtable(matris)
    weightmatriskontrol = 0
    if  rimdf.empty == False :
        weightmatriskontrol = 1
            
    tablokontrol = 0
    if weightmatriskontrol == 1:
        tablokontrol = int(table_control(
            tablename, rimdf["weight_Column"].values[0]))
    elif weightmatriskontrol == 0:
        tablokontrol = int(table_control(tablename))
    print("weightmatriskontrol:", weightmatriskontrol,
          "tablokontrol:", tablokontrol)
    ##################################################################
    if tablokontrol == 1 and weightmatriskontrol == 1:
        print ('Proses başladı...')
        weightcolumn = rimdf["weight_Column"].values[0]
        uniqueid = rimdf["uniqueid"].values[0]
        print("weightcolumn :",weightcolumn,"/ uniqueid :",uniqueid)
        rim_weight(tablename,rimdf,weightcolumn,matris,uniqueid)
        #break
    
    elif tablokontrol == 0 and weightmatriskontrol == 1:
        print (r'[{}] adında bir tablo bulunamadı!!!'.format(tablename))
    elif tablokontrol == 1 and weightmatriskontrol == 0:
        print (r'[{}] adında bir ağırlık matrisi bulunamadı!!!'.format(matris))
    else:
        print ('Tablo ve ağırlık matrisi bilgileri hatalı.Lütfen tekrar deneyiniz.')
        print ('------------------------')


if __name__ == "__main__":
    
    matris = str(sys.argv[1])
    tablename = str(sys.argv[2])
    _weight_run(matris, tablename)

# python c: \Users\emred\Dropbox\Apps\Python\Weight_Data\weight_data_sqledition.py LSCS1639_201811_WEIGHT LSCS1639_WEIGHT_DATASI
