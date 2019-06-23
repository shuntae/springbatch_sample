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
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;

import jp.co.introduction.batch.listener.JobStartEndListener;
import jp.co.introduction.batch.model.FruitInsertModel;
import jp.co.introduction.batch.model.FruitReadCsvModel;
import jp.co.introduction.batch.processor.CsvImportProcessor;

@Configuration
@EnableBatchProcessing
public class CsvImportService {

  @Autowired public JobBuilderFactory jobBuilderFactory;
  @Autowired public StepBuilderFactory stepBuilderFactory;
  @Autowired public DataSource dataSource;

  /**
   * Job<br>
   * ここから後続の処理が呼び出される
   */
  @Bean
  public Job csvImportJob() {
    return jobBuilderFactory
        .get("csvImportJob")
        .incrementer(new RunIdIncrementer())
        // listener呼び出し
        .listener(this.csvImportListener())
        // step(メイン処理)呼び出し
        .flow(this.csvImportStep())
        // 終了
        .end()
        .build();
  }

  /**
   * Listener<br>
   * Jobの開始と終了を管理する
   */
  @Bean
  public JobExecutionListener csvImportListener() {
    return new JobStartEndListener(new JdbcTemplate(dataSource));
  }

  /**
   * Step<br>
   * メイン処理<br>
   * Reader-Processor-Writerを順番に呼び出す
   */
  @Bean
  public Step csvImportStep() {
    return stepBuilderFactory
        .get("step")
        // 処理するデータ型の指定(Fruit)と処理単位の指定(10件ずつ)を行う
        .<FruitReadCsvModel, FruitInsertModel>chunk(10)
        // 処理対象データの読み込み
        .reader(this.csvImportReader())
        // データ加工処理
        .processor(this.csvImportProcessor())
        // データ書き込み処理
        .writer(this.csvImportWriter())
        .build();
  }

  /**
   * Reader<br>
   * CSVのデータを読み込み、stepへ返却する。
   */
  @Bean
  public FlatFileItemReader<FruitReadCsvModel> csvImportReader() {

    // ReaderクラスにCSVの内容を読み込ませる
    FlatFileItemReader<FruitReadCsvModel> reader = new FlatFileItemReader<FruitReadCsvModel>();
    reader.setResource(new ClassPathResource("fruit_price.csv"));

    // CSVデータをFruitInputモデルに変換(フィールド名との紐付け)
    reader.setLineMapper(
        new DefaultLineMapper<FruitReadCsvModel>() {
          {
            setLineTokenizer(
                new DelimitedLineTokenizer() {
                  {
                    setNames(new String[] {"name", "price"});
                  }
                });
            setFieldSetMapper(
                new BeanWrapperFieldSetMapper<FruitReadCsvModel>() {
                  {
                    setTargetType(FruitReadCsvModel.class);
                  }
                });
          }
        });
    return reader;
  }

  /**
   * Processor<br>
   * Readerで読み込んだデータの加工を行う
   */
  @Bean
  public ItemProcessor<FruitReadCsvModel, FruitInsertModel> csvImportProcessor() {
    // FruitItemProcessorでデータの加工処理
    return new CsvImportProcessor();
  }

  /**
   * Writer<br>
   * Processorで加工したデータを読み込みデータベースへの書き込みを行う
   */
  @Bean
  public JdbcBatchItemWriter<FruitInsertModel> csvImportWriter() {
    JdbcBatchItemWriter<FruitInsertModel> writer = new JdbcBatchItemWriter<FruitInsertModel>();
    writer.setItemSqlParameterSourceProvider(
        new BeanPropertyItemSqlParameterSourceProvider<FruitInsertModel>());
    writer.setDataSource(dataSource);

    // FruitInsertModelに定義したフィールド名でplaceHolder(:name)との紐付けを行い、データをバインドする
    writer.setSql("INSERT INTO FRUIT (NAME, PRICE) VALUES (:name, :price)");
    return writer;
  }
}
