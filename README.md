springbatch_sample
============
実行方法：
実行の構成(Run Configuration)の引数(Arguments)のVM引数(VM arguments)に以下を設定して実行

1.CSV取込バッチ(CsvImportService)を実行する場合
---------------------------------------------
-Dspring.batch.job.names=csvImportJob
---------------------------------------------

2.取り込んだデータの更新バッチ(UpdateService)を実行する場合
---------------------------------------------
-Dspring.batch.job.names=updateJob
---------------------------------------------


##### Reference
-   [Creating a Batch Service](http://spring.io/guides/gs/batch-processing/)
-   [Spring Batch Tutorial](https://www.mkyong.com/tutorials/spring-batch-tutorial/)
-   [Spring Batchでバッチ処理](http://dev.classmethod.jp/server-side/java/use_spring-batch_chunk/)
