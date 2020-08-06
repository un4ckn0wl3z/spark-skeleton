from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, IntegerType, DecimalType, StringType
from pyspark.sql.functions import *
from core.logger import log
from core.utils import Util


class SparkManager:

    def __init__(self, config):
        self.order = None
        self.subOrder = None
        self.orderItem = None
        self.data = None
        self.financeOrder = None
        self.test = None
        self.n_to_array = None
        self.data2 = None
        self.data3 = None
        self.full_outer_join = None
        self.testtest = None

        self.paymentFee = 0.02
        self.marketFee = 0.01
        self.platformFee = 0.05

        self.config = config

        self.spark = SparkSession.builder.master(self.config.master).appName(self.config.app_name).getOrCreate()
            # .config(f'spark.mongodb.input.uri={self.config.input_mongodb_uri}') \
            # .config(f'spark.mongodb.output.uri={self.config.output_mongodb_uri}') \
            # .config('spark.driver.extraClassPath', self.config.jars_dir) \
            # .getOrCreate()
        # self.spark.sparkContext.setLogLevel('INFO')

        log.info("=== Spark Info ===")

    def run(self):
        log.info("=== Spark exec now ===")
        self.order = self.spark.read \
            .format("com.mongodb.spark.sql.DefaultSource") \
            .option("collection", "orders") \
            .load()
        self.order = self.order.alias('order')
        # order.limit(1).toPandas()
        self.order.show()
        log.info(Util.getShowString(self.order))

        self.subOrder = self.spark.read \
            .format("com.mongodb.spark.sql.DefaultSource") \
            .option("collection", "orders.suborders") \
            .load()
        self.subOrder = self.subOrder.alias('subOrder')
        # subOrder.select("*").toPandas()
        self.subOrder.show()
        log.info(self.subOrder.show())

        self.orderItem = self.spark.read \
            .format("com.mongodb.spark.sql.DefaultSource") \
            .option("collection", "orders.items") \
            .load()
        # orderItem = orderItem.alias('orderItem')
        self.orderItem.limit(1).toPandas()
        log.info(self.orderItem.limit(1).toPandas())
        # orderItem.show()

        self.data = self.order.join(self.subOrder, self.order._id == self.subOrder.orderId) \
            .join(self.orderItem, self.subOrder._id == self.orderItem.subOrderId) \
            .filter(self.order._id == "hKhl3HlwNMQ")

        self.data.select(["subOrderNo", "productId", "productName", "productPrice", "totalQuantity"]).toPandas()

        self.data = self.subOrder.join(self.orderItem, self.subOrder._id == self.orderItem.subOrderId) \
            .filter(self.subOrder.merchantId == "MEV747qhJ2f")

        self.data.select(["merchantId", "productName", "productPrice", "totalQuantity"]).toPandas()

        self.financeOrder = self.spark.read \
            .format("com.mongodb.spark.sql.DefaultSource") \
            .option("collection", "order.finance") \
            .load()
        # financeOrder = subOrder.alias('subOrder')
        # financeOrder.select("*").toPandas()

        self.financeOrder = self.financeOrder.select(["code", "financeType", "action"])
        self.financeOrder.show()
        log.info(self.financeOrder.show())
        # financeOrder.printSchema()
        # financeOrder = financeOrder.groupBy('action').agg(collect_list('code').alias('code'))
        # testfinanceOrder = financeOrder.select("code")
        # print(testfinanceOrder)
        # testfinanceOrder.show()

        self.test = self.data.select(["merchantName", "totalQuantity", "productPrice"])
        self.test = self.test.withColumn("productItem", lit(1))
        self.test = self.test.withColumn("action", lit("order"))
        self.test.show()
        log.info(self.test.show())

        log.info(self.test.show())

        self.n_to_array = udf(lambda n: [n] * n, ArrayType(IntegerType()))
        self.data2 = self.test.withColumn('totalQuantity', self.n_to_array('totalQuantity'))
        # data2.show()
        self.data3 = self.data2.withColumn('totalQuantity', explode('totalQuantity'))
        # # data3.printSchema()
        # data3 = data3.select("*")
        self.data3.show()
        log.info(self.data3.show())
        # ppp = financeOrder.crossJoin(data3)
        # # ppp = data3.crossJoin(financeOrder)
        # ppp.orderBy('merchantName', ascending=False).show()
        # df = df1.join(df2, on=['key'], how='inner')

        # df.show()
        # daaa = data3.join(financeOrder, data3.action == financeOrder.action).select("*")
        # # daaa.printSchema()
        # daaa.show(1)

        self.full_outer_join = self.data3.join(self.financeOrder, self.data3.action == self.financeOrder.action,
                                     how='full')  # Could also use 'full_outer'
        self.full_outer_join.select(["merchantName", "productPrice", "productItem", "code", "financeType"]).show()

        self.testtest = self.full_outer_join.withColumn('Amount', when((col("financeType") == "paymentFee"),
                                                             round((col('productPrice') * self.paymentFee), 2))
                                              .when((col("financeType") == "marketFee"),
                                                    round((col('productPrice') * self.marketFee), 2))
                                              .when((col("financeType") == "platformFee"),
                                                    round((col('productPrice') * self.platformFee), 2))
                                              .otherwise(col('productPrice'))
                                              )
        self.testtest.show()
        log.info(self.testtest.show())




