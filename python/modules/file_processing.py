# External libraries
from logging import exception as log_exception, info as log_info
from pandas import DataFrame, read_csv, set_option

# Internal modules
from modules.setup import Config

set_option('mode.chained_assignment', None)

class FileProcessing():

    def __init__(self, configuration: Config) -> None:
        self.dtypes_dict = configuration.cfg['file_types']
        self.keys = configuration.cfg['keys']
        raw_df = self.read_file(configuration.cfg['general']['process_file'])
        self.main_df    = self.set_dtypes(raw_df)

        self.create_dim_tables()
        self.create_fact_table()

    def read_file(self, file_name: str) -> DataFrame:
        # Create a basic dataframe with the raw data
        # We skip the first 9 rows as these contain the report headers
        log_info('Reading CSV file')

        try:
            df =   read_csv(file_name, skiprows=9)

            # We drop the last row of the df as it contains the report footer
            df = df[:-1]
            return df
        except Exception as e:
            log_exception(e)

    def set_dtypes(self, df: DataFrame) -> DataFrame:
        '''
            This method takes a 'raw' dataframe, and sets the dtypes of all columns, this was determined through a manual analysis

            Args:
                df = Pandas DataFrame, created during the class instantiation
        '''
        log_info('Updating data types')
        try:
            for key, value in self.dtypes_dict.items():
                df[key] = df[key].astype(value)

            # As the below two columns contain data with extra characters, we need to remove those before setting the dtype
            for column in ['Ad group ID', 'Ad ID']:
                df[column] = df[column].str.strip('[]').astype('Int64')
            return df
        except Exception as e:
            log_exception(e)
    
    def create_dim_tables(self):
        '''
            This method servers to define all dimensional tables based on a manual analysis
        '''
        log_info('Generating dimensional tables')
        try:
            self.main_df.replace(['None', 'nan'], '', inplace=True)

            self.df_dim_account = self.main_df[['Account number', 'Account name', 'Account status']].sort_values('Account number').drop_duplicates(ignore_index=True)
            self.df_dim_ad_group = self.main_df[['Ad group ID', 'Ad group', 'Ad group status', 'Language', 'Currency code']].sort_values('Ad group ID').drop_duplicates(ignore_index=True)
            self.df_dim_ads = self.main_df[['Ad ID', 'Ad group ID', 'Ad description', 'Ad distribution', 'Ad status', 'Ad title', 'Ad type']].sort_values('Ad ID').drop_duplicates(ignore_index=True)

            self.df_dim_customer = self.main_df[['Customer', 'Campaign name', 'Campaign status']].sort_values('Campaign name').drop_duplicates(ignore_index=True)
            self.df_dim_customer.insert(0, self.keys['c_key'], self.df_dim_customer.index)
            self.df_dim_customer[self.keys['c_key']] = self.df_dim_customer[self.keys['c_key']].astype('int16')
            
            self.df_dim_device = self.main_df[['Device type', 'Device OS']].sort_values('Device type').drop_duplicates(ignore_index=True)
            self.df_dim_device.insert(0, self.keys['d_key'], self.df_dim_device.index)
            self.df_dim_device[self.keys['d_key']] = self.df_dim_device[self.keys['d_key']].astype('int16')

            self.df_dim_network = self.main_df[['Network', 'Top vs. other']].sort_values('Top vs. other').drop_duplicates(ignore_index=True)
            self.df_dim_network.insert(0, self.keys['n_key'], self.df_dim_network.index)
            self.df_dim_network[self.keys['n_key']] = self.df_dim_network[self.keys['n_key']].astype('int16')

            df_dim_urls = self.main_df[['Display URL', 'Tracking Template', 'Final App URL', 'Final Mobile URL', 'Custom Parameters']]
            df_dim_urls['Navigation URL'] = self.main_df['Destination URL'] + self.main_df['Final URL']
            
            self.df_dim_urls = df_dim_urls.drop_duplicates(ignore_index=True)
            self.df_dim_urls.insert(0, self.keys['u_key'], self.df_dim_urls.index)
            self.df_dim_urls[self.keys['u_key']] = self.df_dim_urls[self.keys['u_key']].astype('Int16')
            
        except Exception as e:
            log_exception(e)
            
    def create_fact_table(self):
        from pandas import merge

        log_info('Generating fact table')
        try:
            # Creating a copy of the original dataframe
            df_tmp_fct = self.main_df

            # Merging these columns 
            df_tmp_fct['Navigation URL'] = self.main_df['Destination URL'] + self.main_df['Final URL']

            # Dropping unnecessary columns
            df_tmp_fct = df_tmp_fct.drop(columns=['Account name', 'Account status', 'Ad group ID', 'Ad group', 'Ad group status', 'Ad description',
                'Ad distribution', 'Ad status', 'Ad title', 'Ad type', 'Language', 'Currency code', 'Final URL',  'Destination URL'])

            # Replacing URL data for a single key
            columns = ['Tracking Template', 'Navigation URL', 'Display URL']
            df_tmp_fct = merge(df_tmp_fct, self.df_dim_urls, how='left', left_on=columns, right_on=columns)
            df_tmp_fct = df_tmp_fct.drop(columns= ['Final Mobile URL_x', 'Final App URL_x', 'Final App URL_y', 'Final Mobile URL_y', 'Custom Parameters_x', 'Custom Parameters_y'])
            df_tmp_fct = df_tmp_fct.drop(columns=columns)

            # Replacing Device data
            columns = ['Device type', 'Device OS']
            df_tmp_fct = merge(df_tmp_fct, self.df_dim_device, how='left', left_on=columns, right_on=columns)
            df_tmp_fct = df_tmp_fct.drop(columns=columns)

            # Replacing Network data
            columns = ['Top vs. other', 'Network']
            df_tmp_fct = merge(df_tmp_fct, self.df_dim_network, how='left', left_on=columns, right_on=columns)
            df_tmp_fct = df_tmp_fct.drop(columns= columns)

            # Replacing Customer data
            columns = ['Customer', 'Campaign name']
            df_tmp_fct = merge(df_tmp_fct, self.df_dim_customer, how='left', left_on=columns, right_on=columns)
            df_tmp_fct = df_tmp_fct.drop(columns= ['Campaign status_x', 'Campaign status_y'])
            df_tmp_fct = df_tmp_fct.drop(columns=columns)

            # Fixing dtypes
            df_tmp_fct[self.keys['c_key']] = df_tmp_fct[self.keys['c_key']].astype('int16')
            df_tmp_fct[self.keys['d_key']] = df_tmp_fct[self.keys['d_key']].astype('int16')
            df_tmp_fct['Network Key'] = df_tmp_fct['Network Key'].astype('int16')
            df_tmp_fct['URL Key'] = df_tmp_fct['URL Key'].astype('Int16')

            self.df_fct_stats = df_tmp_fct.sort_values('Gregorian date', ignore_index=True)
        except Exception as e:
            log_exception(e)