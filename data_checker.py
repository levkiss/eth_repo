import pandas as pd
import configparser
import subprocess
import os
from datetime import datetime, timedelta
from web3 import Web3
from clickhouse_driver import Client


class EthETLExecutor:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('config.ini')
        self.url = self.config['DEFAULT']['url']
        self.infura_url = self.config['DEFAULT']['infura_url']
        self.batch_size = self.config['DEFAULT']['batch_size']
        self.w3 = self.initialize_infura_connection()

    def initialize_infura_connection(self):
        w3 = Web3(Web3.HTTPProvider(self.infura_url))
        res = w3.isConnected()
        if res:
            return w3

    def get_blocks_and_transactions(self, start_block, end_block, blocks_out, transactions_out):
        process = subprocess.Popen([
            'ethereumetl', 'export_blocks_and_transactions',
            '--start-block', str(start_block),
            '--end-block', str(end_block),
            '--provider-uri', 'https://mainnet.infura.io/v3/7aef3f0cd1f64408b163814b22cc643c',
            '--blocks-output', blocks_out,
            '--transactions-output', transactions_out
        ],
            stdout=subprocess.PIPE
        )
        stdout, stderr = process.communicate()
        return  # first_block, last_block

    def get_transactions_by_blocks(self, start_block, last_block, transactions_output):
        process = subprocess.Popen(
            ['ethereumetl', 'export_blocks_and_transactions',
             '--start-block', start_block,
             '--end-block', last_block,
             '--provider-uri', self.url,
             '--transactions-output', transactions_output],
            stdout=subprocess.PIPE
        )
        stdout, stderr = process.communicate()
        return

    def get_hashes(self, transactions_file, output_hashes):
        process = subprocess.Popen(
            ['ethereumetl', 'extract_csv_column',
             '--input', transactions_file,
             '--column', 'hash',
             '--output', output_hashes
             ],
            stdout=subprocess.PIPE
        )
        stdout, stderr = process.communicate()
        return

    def get_receipts_and_logs(self, transaction_hashes, receipts_output, logs_output):
        process = subprocess.Popen(
            ['ethereumetl', 'export_receipts_and_logs',
             '--transaction-hashes', transaction_hashes,
             '--provider-uri', self.url,
             '--receipts-output', receipts_output,
             '--logs-output', logs_output
             ],
            stdout=subprocess.PIPE
        )
        stdout, stderr = process.communicate()
        return

    def get_token_transfers(self, logs_file, token_transfers_output):
        process = subprocess.Popen(
            ['ethereumetl', 'extract_token_transfers',
             '--logs', logs_file,
             '--output', token_transfers_output
             ],
            stdout=subprocess.PIPE
        )
        stdout, stderr = process.communicate()
        return

    def get_contract_addresses(self, receipts_file, contract_addresses):
        process = subprocess.Popen(
            ['ethereumetl', 'extract_csv_column',
             '--input', receipts_file,
             '--column', 'contract_address',
             '--output', contract_addresses
             ],
            stdout=subprocess.PIPE
        )
        stdout, stderr = process.communicate()
        return

    def export_contracts(self, contract_addresses, contracts_file):
        process = subprocess.Popen(
            ['ethereumetl', 'export_contracts',
             '--contract-addresses', contract_addresses,
             '--provider-uri', self.url,
             '--output', contracts_file
             ],
            stdout=subprocess.PIPE
        )
        stdout, stderr = process.communicate()
        return

    def download_eth_in_range(self, start, end):
        self.get_blocks_and_transactions(start, end, 'data/blocks.csv', 'data/transactions.csv')
        self.get_hashes('data/transactions.csv', 'data/transaction_hashes.txt')
        self.get_receipts_and_logs('data/transaction_hashes.txt', 'data/receipts.csv', 'data/logs.csv')
        self.get_token_transfers('data/logs.csv', 'data/token_transfers.csv')
        self.get_contract_addresses('data/receipts.csv', 'data/contract_addresses.txt')
        self.export_contracts('data/contract_addresses.txt', 'data/contracts.csv')
        return

    def download_eth_full_batches(self):
        pass

    def download_eth_block(self, block_num):
        pass


class DataLoader(EthETLExecutor):
    def __init__(self):
        super().__init__()
        self.click_client = self.get_connection_clickhouse()
        self.batch_size = int(self.config['DEFAULT']['batch_size'])

    def sort_transactions_csv(self):
        chunksize = 5 * 10 ** 3
        path = 'data/transactions.csv'
        # path_out_big = 'data/transactions_big.csv'
        path_out_small = 'data/transactions_clean.csv'

        for chunk in pd.read_csv(path, chunksize=chunksize):
            chunk.block_timestamp = pd.to_datetime(chunk.block_timestamp, unit='s')
            # df_big = chunk.loc[chunk.input.str.len() > 5000][['hash', 'input']]
            chunk.loc[chunk.input.str.len() > 5000, 'input'] = 'big'
            chunk = chunk.convert_dtypes()
            chunk.to_csv(path_out_small, index=False, header=False, mode='a')
            # df_big.to_csv(path_out_big, index=False, header=False, mode='a')
        return

    def delete_first_row_of_csv(self, filename):
        os.system(f"sed -i '1d' {filename}")
        return

    def get_connection_clickhouse(self):
        client = Client(host='localhost')
        return client

    def get_max_block_clickhouse(self):
        block = self.click_client.execute(
            f"SELECT greatest(max(number),{self.config['DEFAULT']['start_block']}) FROM STG_ETH.BLOCKS;"
        )[0][0]
        return block

    def get_max_eth_block(self):
        return self.w3.eth.block_number

    def write_file_to_clickhouse(self, table, filename):
        '''
        with open(filename, 'r') as f:
            # data = f.read()
            process = subprocess.Popen(
                ['clickhouse-client',
                 '--format_csv_delimiter=","',
                 f'--query=INSERT INTO {table} FORMAT CSV'
                 ],
                stdin=f,
                stdout=subprocess.PIPE
            )
        stdout, stderr = process.communicate()
        '''
        sql = f'clickhouse-client --format_csv_delimiter="," --query="INSERT INTO {table} FORMAT CSV" < {filename}'
        os.system(sql)
        return

    def write_data_to_clickhouse(self):
        files_list = {
            'STG_ETH': {
                'BLOCKS': 'blocks.csv',
                'LOGS': 'logs.csv',
                'RECEIPTS': 'receipts.csv',
                'TOKEN_TRANSFERS': 'token_transfers.csv'}
        }

        # first we write transactions to db
        self.sort_transactions_csv()
        self.write_file_to_clickhouse('STG_ETH.TRANSACTIONS', 'data/transactions_clean.csv')
        os.remove('data/transactions_clean.csv')

        for i in files_list.keys():
            for j in files_list[i].keys():
                self.delete_first_row_of_csv(f'data/{files_list[i][j]}')
                self.write_file_to_clickhouse(f'{i}.{j}', f'data/{files_list[i][j]}')

        return

    def download_data(self):
        max_block_clickhouse = self.get_max_block_clickhouse()
        max_block_eth = self.get_max_eth_block()
        delta = max_block_eth - max_block_clickhouse
        if delta < self.batch_size:
            # here we download files
            self.download_eth_in_range(max_block_clickhouse, max_block_eth)
            # clean data
            # load data
            self.write_data_to_clickhouse()
        else:
            start = max_block_clickhouse
            end = max_block_clickhouse + self.batch_size
            while start < max_block_eth:
                end = min(end, max_block_eth)
                self.download_eth_in_range(start, end)
                # clean data
                # load data
                self.write_data_to_clickhouse()
                start += self.batch_size
                end += self.batch_size

    def download_example(self):
        self.download_eth_in_range(15270500, 15271000)
        self.write_data_to_clickhouse()


if __name__ == '__main__':
    # loader = EthETLExecutor()
    # loader.get_blocks_and_transactions(15270380, 15270390, 'data/blocks.csv', 'data/transactions.csv')
    # loader.get_hashes('data/transactions.csv', 'data/transaction_hashes.txt')
    # loader.get_receipts_and_logs('data/transaction_hashes.txt', 'data/receipts.csv', 'data/logs.csv')
    # loader.get_token_transfers('data/logs.csv', 'data/token_transfers.csv')
    # loader.get_contract_addresses('data/receipts.csv', 'data/contract_addresses.txt')
    # loader.export_contracts('data/contract_addresses.txt', 'data/contracts.csv')
    # loader.download_eth_in_range(15270380, 15270390)
    loaderr = DataLoader()
    # loaderr.download_eth_in_range(15270380, 15270500)
    #loaderr.download_example()
    #loaderr.write_data_to_clickhouse()
    #loaderr.sort_transactions_csv()
    loaderr.get_receipts_and_logs('data/transaction_hashes.txt', 'data/receipts.csv', 'data/logs.csv')
