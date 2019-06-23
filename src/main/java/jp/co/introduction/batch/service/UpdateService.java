package jp.co.introduction.batch.service;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import jp.co.introduction.batch.entity.FruitEntity;
import jp.co.introduction.batch.entity.FruitRowMapper;
import jp.co.introduction.batch.listener.JobStartEndListener;
import jp.co.introduction.batch.model.FruitUpdateModel;
import jp.co.introduction.batch.processor.UpdateServiceProcessor;

@Configuration
@EnableBatchProcessing
public class UpdateService {

  @Autowired public JobBuilderFactory jobBuilderFactory;
  @Autowired public StepBuilderFactory stepBuilderFactory;
  @Autowired public DataSource dataSource;

  /**
   * Job<br>
   * ここから後続の処理が呼び出される
   */
  @Bean
  public Job updateJob() {
    return jobBuilderFactory
        .get("updateJob")
        .incrementer(new RunIdIncrementer())
        // listener呼び出し
        .listener(this.updateServiceListener())
        // step(メイン処理)呼び出し
        .flow(this.updateServiceStep())
        // 終了
        .end()
        .build();
  }

  /**
   * Listener<br>
   * Jobの開始と終了を管理する
   */
  @Bean
  public JobExecutionListener updateServiceListener() {
    return new JobStartEndListener(new JdbcTemplate(dataSource));
  }

  /**
   * Step<br>
   * メイン処理<br>
   * Reader-Processor-Writerを順番に呼び出す
   */
  @Bean
  public Step updateServiceStep() {
    return stepBuilderFactory
        .get("step")
        // 処理するデータ型の指定(Fruit)と処理単位の指定(10件ずつ)を行う
        .<FruitEntity, FruitUpdateModel>chunk(10)
        // 処理対象データの読み込み
        .reader(this.updateServiceReader())
        // データ加工処理
        .processor(this.updateServiceProcessor())
        // データ書き込み処理
        .writer(this.updateServiceWriter())
        .build();
  }

  /**
   * Reader<br>
   * DBのデータを読み込み、stepへ返却する。
   */
  @Bean
  public ItemReader<FruitEntity> updateServiceReader() {
    return new JdbcCursorItemReaderBuilder<FruitEntity>()
        .dataSource(dataSource)
        .name("jdbc-reader")
        .sql("SELECT ID, NAME, PRICE, COMMENT FROM FRUIT")
        .rowMapper(new FruitRowMapper())
        .build();
  }

  /**
   * Processor<br>
   * Readerで読み込んだデータの加工を行う
   */
  @Bean
  public ItemProcessor<FruitEntity, FruitUpdateModel> updateServiceProcessor() {
    // ProcessedServiceProcessorでデータの加工処理
    return new UpdateServiceProcessor();
  }

  /**
   * Writer<br>
   * Processorで加工したデータを読み込みデータベースへ更新を行う
   */
  @Bean
  public JdbcBatchItemWriter<FruitUpdateModel> updateServiceWriter() {
    JdbcBatchItemWriter<FruitUpdateModel> writer = new JdbcBatchItemWriter<FruitUpdateModel>();
    writer.setItemSqlParameterSourceProvider(
        new BeanPropertyItemSqlParameterSourceProvider<FruitUpdateModel>());
    writer.setDataSource(dataSource);

    // FruitUpdateModelに定義したフィールド名でplaceHolder(:name)との紐付けを行い、データをバインドする
    writer.setSql("UPDATE FRUIT SET COMMENT = :comment WHERE ID = :id ");
    return writer;
  }
}
