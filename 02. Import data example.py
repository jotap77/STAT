import pandas as pd
import pyodbc
import urllib
import sqlalchemy
from sqlalchemy import event

# Import CSV
data = pd.read_csv (r'G:\...',sep=';',header=0,decimal='.',names = ['Chave', 'Periodo', 'Valor'],dtype = {'Chave':str, 'Periodo':str, 'Valor':'float64'})   
df = pd.DataFrame(data)
df['Valor'] = df['Valor'].round(decimals=6)

params = 'Driver={ODBC Driver 17 for SQL Server};Server=STAT;Database=ABC;Trusted_Connection=yes;autocommit=False;'
conn = pyodbc.connect(params)
cursor = conn.cursor()

db_params = urllib.parse.quote_plus(params)
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(db_params))

@event.listens_for(engine, "before_cursor_execute")
def receive_before_cursor_execute(
       conn, cursor, statement, params, context, executemany
        ):
            if executemany:
                cursor.fast_executemany = True

df.to_sql('EFI_SeriesBCE_Completo', engine, index=False, if_exists="replace", schema="IVF", chunksize=100000)

stmt = "ALTER TABLE IVF.EFI_Series ALTER COLUMN Chave nvarchar(50)"
cursor.execute(stmt)
conn.commit()

stmt = "ALTER TABLE IVF.EFI_Series ALTER COLUMN Periodo nvarchar(7)"
cursor.execute(stmt)
conn.commit()

stmt = "ALTER TABLE IVF.EFI_Series ALTER COLUMN Valor numeric(26, 6)"
cursor.execute(stmt)
conn.commit()

conn.close()
engine.dispose()

print('CSV done')
