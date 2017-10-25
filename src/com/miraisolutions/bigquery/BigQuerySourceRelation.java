/**
  * Relation for Google BigQuery data source
 *
  * @param tableRef BigQuery table reference
  * @param sqlContext Spark SQL context
  */
        private[sbbhistory] case class BigQuerySourceRelation(
            tableRef: TableReference,
            val sqlContext: SQLContext
                ) extends BaseRelation with PrunedFilteredScan {

                  private lazy val table = sqlContext.bigQueryTable(tableRef)

                  override def schema: StructType = table.schema

                  // TODO: Implement pruning/filtering; see also JDBCRDD
                  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
                    table.rdd
                          }
                }
