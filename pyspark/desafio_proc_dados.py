from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, split
from pyspark.sql.types import DoubleType

class ProcessadorVendas:
    """
    Classe para processamento de vendas.
    """
    def __init__(self, spark, file_path):
        """
        Inicializa o ProcessadorVendas.
        """
        self.spark = spark
        self.file_path = file_path
        self.df = self.carregar_dataframe()
    
    def carregar_dataframe(self):
        """
        Carrega o DataFrame a partir do arquivo CSV.
        """
        return self.spark.read.csv(self.file_path, header=True, inferSchema=True)
    
    def produto_mais_vendido(self):
        """
        Identifica o produto mais vendido em termos de quantidade e canal.
        """
        vendas_produto_canal = self.df.groupBy('Item Type', 'Sales Channel') \
                                      .agg(sum('Units Sold').alias('TotalQuantidade'))
        produto_max_vendas = vendas_produto_canal.orderBy(col("TotalQuantidade").desc()).first()
        return (produto_max_vendas['Item Type'], produto_max_vendas['Sales Channel']), produto_max_vendas['TotalQuantidade']
    
    def maior_volume_vendas(self):
        """
        Determina o país e região com o maior volume de vendas.
        """
        vendas_pais_regiao = self.df.groupBy('Country', 'Region') \
                                    .agg(sum('Total Revenue').alias('TotalVolumeVendas'))
        maior_volume = vendas_pais_regiao.orderBy(col("TotalVolumeVendas").desc()).first()
        return (maior_volume['Country'], maior_volume['Region']), maior_volume['TotalVolumeVendas']
    
    def media_vendas_mensais(self):
        """
        Calcula a média de vendas mensais por produto.
        """
        self.df = self.df.withColumn('Month', split(col("Order Date"), "\s|/").getItem(0))
        self.df = self.df.withColumn('Total Revenue', self.df['Total Revenue'].cast(DoubleType()))
        vendas_mensais = self.df.groupBy('Item Type', 'Month') \
                                .agg(sum('Units Sold').alias('TotalQuantidade'))
        total_meses = vendas_mensais.select('Month').distinct().count()
        media_vendas_mensais = vendas_mensais.groupBy('Item Type') \
                                             .agg(sum('TotalQuantidade').alias('TotalQuantidade')) \
                                             .withColumn('MediaVendasMensais', col('TotalQuantidade') / total_meses)
        return media_vendas_mensais.select('Item Type', 'MediaVendasMensais').rdd.collectAsMap()

# Inicializando a sessão Spark
spark = SparkSession.builder \
    .appName("Desafio de Processamento") \
    .getOrCreate()

# Definindo o caminho para o arquivo CSV de grandes vendas
file_path = 'C:/Users/diego/Downloads/Teste_Engenheiro_de_Dados/vendas.csv'

# Criando uma instância do ProcessadorVendas
processador = ProcessadorVendas(spark, file_path)

# Chamando as funções para resolver os problemas propostos
produto_mais_vendido_info = processador.produto_mais_vendido()
maior_volume_vendas_info = processador.maior_volume_vendas()
media_vendas_mensais_info = processador.media_vendas_mensais()

# Exibindo os resultados
print("Produto mais vendido em termos de quantidade e canal:", produto_mais_vendido_info)
print("País e região com o maior volume de vendas:", maior_volume_vendas_info)
print("Média de vendas mensais por produto:", media_vendas_mensais_info)

# Encerrando a sessão Spark
spark.stop()
