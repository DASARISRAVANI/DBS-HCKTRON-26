import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession, types}

object data_cleaning {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .appName("test")
      .master("local")
      .getOrCreate()

    val customerfile = "/Users/muddu/Desktop/dbs_data/Customer.txt"
    val customerString = "customer_id customer_first_name customer_last_name phone_number"
    val customerfile = "/Users/muddu/Desktop/dbs_data/Customer.txt"
    val customerString = "customer_id customer_first_name customer_last_name phone_number"
    val customerfile = "/Users/muddu/Desktop/dbs_data/Customer.txt"
    val customerString = "customer_id customer_first_name customer_last_name phone_number"
    val customerfile = "/Users/muddu/Desktop/dbs_data/Customer.txt"
    val customerString = "customer_id customer_first_name customer_last_name phone_number"

    def load_data (schema1: String, data: String) : DataFrame  = {
      val fields = schema1.split(" ").map(fieldName => StructField(fieldName, StringType, nullable=true))
      val schema = StructType(fields)
      val df = ss.read
        .option("header",false)
        .option("delimiter", "|")
        .schema(schema)
        .csv(data)
      return(df)
    }


    val customerdf =  load_data(customerString,customerfile)
    customerdf.show()

  }

}

