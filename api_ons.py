import requests
import boto3
import json
import s3fs
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class ApiOns:
    def __init__(self):
        self.base_url = "https://apicarga.ons.org.br/prd"

    def get_carga_verificada(
        self, data_inicio, data_fim, cod_areacarga
    ) -> requests.Response:
        url = self.base_url + "/cargaverificada"
        params = {
            "dat_inicio": data_inicio,
            "dat_fim": data_fim,
            "cod_areacarga": cod_areacarga,
        }
        response = requests.get(url, params=params, timeout=10)
        content = json.loads(response.content.decode("utf-8"))
        return content

    def get_carga_programada(
        self, data_inicio, data_fim, cod_areacarga
    ) -> requests.Response:
        url = self.base_url + "/cargaprogramada"
        params = {
            "dat_inicio": data_inicio,
            "dat_fim": data_fim,
            "cod_areacarga": cod_areacarga,
        }
        response = requests.get(url, params=params, timeout=10)
        content = json.loads(response.content.decode("utf-8"))
        return content


class ApiWrapper:
    def __init__(self, scraping_interval_days=1):
        
        self.scraping_interval_days = scraping_interval_days
        self.api = ApiOns()
        self.s3_bucket_name = "ons-api-power-data"
        self.boto_client = boto3.client("s3")

    def create_s3_bucket(self):
        """
        Creates a S3 Bucket if it does not exist
        """
        
        #Checks if the bucket exists
        try:
            self.boto_client.head_bucket(Bucket=self.s3_bucket_name)
        except:
            #Creates the bucket
            self.boto_client.create_bucket(Bucket=self.s3_bucket_name)
            print("Bucket created")
        else:
            print("Bucket already exists")

    #def get_partition

    def read_data_parquet(self, cod_areacarga, year, month, day):
        """
        Reads a parquet file from a S3 bucket
        """
        s3_path = f"s3://{self.s3_bucket_name}/test-partition/data.parquet/cod_areacarga={cod_areacarga}/val_year={year}/val_month={month}/val_day={day}/"

        df = pd.read_parquet(s3_path, engine="pyarrow")

        return df

    def save_data_parquet(self, df):
        """
        Saves a dataframe as parquet file in a S3 bucket
        """
        s3_path = f"s3://{self.s3_bucket_name}/test-partition/data.parquet"

        # Criar um sistema de arquivos S3
        fs = s3fs.S3FileSystem()

        # Converts dt_referencia to datetime
        df["dat_referencia"] = pd.to_datetime(df["dat_referencia"])

        # Partiton into year, month, day
        df["val_year"] = df["dat_referencia"].dt.year
        df["val_month"] = df["dat_referencia"].dt.month
        df["val_day"] = df["dat_referencia"].dt.day

        df.to_parquet(
            s3_path,
            engine="pyarrow",
            filesystem=fs,  # Passa o sistema de arquivos S3FS
            partition_cols=["cod_areacarga", "val_year", "val_month", "val_day"], # Needs to change to year, month, day,
            existing_data_behavior="delete_matching",
            compression="snappy",
        )

        return

    



if __name__ == "__main__":

    api = ApiOns()

    # print("Teste de carga verificada")
    resp_verificada = api.get_carga_verificada("2021-01-03", "2021-01-04", "SP")
    # print(resp_verificada.status_code)
    # if resp_verificada.status_code == 200:
    #     print(resp_verificada.content)

    # print("Teste de carga programada")
    # resp_programada = api.get_carga_verificada("2021-01-01", "2021-01-02", "SP")
    # print(resp_programada.status_code)
    # if resp_programada.status_code == 200:
    #     print(resp_programada.content)


    api_wrapper = ApiWrapper()
    api_wrapper.create_s3_bucket()

    df = pd.DataFrame(resp_verificada)
