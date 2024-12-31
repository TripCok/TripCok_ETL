current_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(current_dir, "place.csv")

ml_map_df = self.spark.read.option("header", "true").csv(file_path)
df = self.spark.read.parquet(self.s3_path)

df = df.withColumn("parsed_response", from_json(col("response"), self.get_schema()))

# parsed_response에서 데이터 추출
df = (df.select(
    col("*"),  # 기존 모든 칼럼
    col("parsed_response.id").alias("id"),
    # col("parsed_response.name").alias("name"),
    # col("parsed_response.categoryNames").alias("categoryNames")
).drop("parsed_response").drop("response").drop("clientIp").drop("url")
      .drop("requestParam").drop("request").drop("statusCode").drop("time"))

df.show(n=10, truncate=False)
ml_map_df = ml_map_df.filter(col("id").cast("int").isNotNull())
ml_map_df = (ml_map_df.select(
    col("id").alias("id"),
    # col("name").alias("name"),
    col("ml_mapping_id").alias("ml_mapping_id"),
    # col("address").alias("address"),
).drop("latitude").drop("longitude").drop("time").drop("create_time").drop("update_time"))

ml_map_df.show(n=10, truncate=False)

joined_df = df.join(ml_map_df, on="id", how="inner")

joined_df.show()
# 윈도우 정의
window_spec = Window.partitionBy("memberId").orderBy(col("counts").desc())

# counts 계산
grouped_df = joined_df.groupBy("memberId", "ml_mapping_id").agg(count("*").alias("counts"))

# 순위 계산
ranked_df = grouped_df.withColumn("rank", row_number().over(window_spec))
# ranked_df.printSchema()

# rank가 1인 행만 필터링
top_counts_df = ranked_df.filter(col("rank") == 1).select("memberId", "ml_mapping_id", "counts")

top_counts_df.show()

result_df = top_counts_df.join(joined_df.select("traceId", "memberId", "ml_mapping_id", "etl_dtm"),
                               on=["memberId", "ml_mapping_id"], how="left").orderBy("traceId")

# result_df.show(truncate=False)

result_df = result_df.dropDuplicates(["memberId", "ml_mapping_id"])

# result_df.show(truncate=False)
