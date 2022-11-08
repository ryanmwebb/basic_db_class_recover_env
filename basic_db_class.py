import json
from io import StringIO
import psycopg2
import pandas as pd

class simple_db():
    '''
    barebones, probably inefficient implementation of a reusable db class
    @TODO Address the many opportunities for sql injections
    '''
    #obviously dumb, but code imitates life? Not even used currently
    CONN_PARAMS=set(['host','hostaddr','port','dbname','user',
    'password','passfile','channel_binding','connect_timeout',
    'client_encoding','options','application_name',
    'fallback_application_name','keepalives','keepalives_idle',
    'keepalives_interval','keepalives_count','tcp_user_timeout',
    'replication','gssencmode','sslmode','requiressl',
    'sslcompression','sslcert','sslkey','sslpassword','sslrootcert',
    'sslcrl','sslcrldir','sslsni','requirepeer',
    'ssl_min_protocol_version','ssl_max_protocol_version','krbsrvname',
    'gsslib','service','target_session_attrs'])

    def __init__(self,connection_file):
        self.conn = self.remote_connect(connection_file)

    def __clean_creds__(self, credentials_dict):
        '''
        @TODO In a real version this should be a uri string.
        also, it should check against valid pg connection parameters
        to notify a user if they submit an invalid parameter and should require
        the core parameters (those currently included in the if statement + ssl, imo).
        also also, I currently use our existing argos files to connect, 
        which introduces some clunkiness because obviously those are meant for srcr
        '''
        cleaned_dict={}
        for k in credentials_dict.keys():
            if k in ['host','port','dbname','user','password']:
                cleaned_dict[k]=credentials_dict[k]
        return cleaned_dict

    def remote_connect(self, json_path):
        '''
        has hardcoded keys from argos rather than arbitrary ones for now
        @TODO set it up with uri string or a .ini instead
        '''
        with open(json_path) as db_creds:
            creds=json.load(db_creds)['src_args']
        clean_creds=self.__clean_creds__(creds)
        return psycopg2.connect(host=clean_creds['host'],
                 database=clean_creds['dbname'],
                 user=creds['user'],
                 password=creds['password'])

    def role_change(self,role):
        '''
        swap roles after connection, always connect as yourself and 
        change roles later if you want.

        @TODO if pedsnet adoption, charlie won't like this, decide
        whether to fight or accomodate
        '''
        with self.conn.cursor() as cur:
            try:
                cur.execute('SET ROLE {}'.format(role))
                self.conn.commit()
            except Exception as e:
                print(e)
                self.conn.rollback()
                return e

    def query_no_return(self, query_string):
        '''
        begging for sql injection, dcc without walls babyyyy! 
        '''
        with self.conn.cursor() as cur:
            try:
                cur.execute(query_string)
                self.conn.commit()
            except (Exception, psycopg2.DatabaseError) as e:
                print(e)
                self.conn.rollback()
                return e

    def query_return_df(self,query_string: str, df_cols: list,df_col_types: dict):
        '''
        inject here too if you want our data!
        To make a thief make an owner amiright?!

        also sorry future ryan for not static typing my other methods
        @TODO static type the other methods
        @TODO make this work less painfully for complex queries
        @TODO add stuff for temp tables
        '''
        with self.conn.cursor() as cur:
            try:
                cur.execute(query_string) 
                df = pd.DataFrame(cur.fetchall(),columns=df_cols).astype(df_col_types)
                self.conn.commit()
                return df
            except Exception as e:
                print(e)
                self.conn.rollback()
                return e

    def bulk_insert_stringio(self, df, table):
        '''
        write an in memory csv (avoiding disk!) and copy it to an existing database table

        @TODO chunk this to accomodate larger tables
        '''
        buffer = StringIO()
        buffer.write(df.to_csv(header=True, index=False))
        buffer.seek(0)
        with self.conn.cursor() as cur:
            try:
                load_string = "COPY {} FROM STDIN DELIMITER ',' CSV HEADER;".format(table)
                cur.copy_expert(load_string,buffer)
                self.conn.commit()
            except (Exception, psycopg2.DatabaseError) as e:
                self.conn.rollback()
                print(e)
                return e

    def brick_it_for_susan_and_alex(self):
        # @TODO figure out where to close automatically, if ever. 
        self.conn.close()
        print('this instance is no longer functional, the connection is permanently closed')