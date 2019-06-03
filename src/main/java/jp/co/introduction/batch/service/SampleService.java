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
import jp.co.introduction.batch.model.FruitInput;
import jp.co.introduction.batch.model.FruitOutput;
import jp.co.introduction.batch.processor.FruitItemProcessor;

@Configuration
@EnableBatchProcessing
public class SampleService {

  @Autowired
  public JobBuilderFactory jobBuilderFactory;
  @Autowired
  public StepBuilderFactory stepBuilderFactory;
  @Autowired
  public DataSource dataSource;

  /**
   * Job<br>
   * ここから後続の処理が呼び出される
   */
  @Bean
  public Job sampleJob() {
    return jobBuilderFactory.get("sampleJob").incrementer(new RunIdIncrementer())
        // listener呼び出し
        .listener(this.listener())
        // step(メイン処理)呼び出し
        .flow(this.step())
        // 終了
        .end().build();
  }

  /**
   * Listener<br>
   * Jobの開始と終了を管理する
   */
  @Bean
  public JobExecutionListener listener() {
    return new JobStartEndListener(new JdbcTemplate(dataSource));
  }

  /**
   * Step<br>
   * メイン処理<br>
   * Reader-Processor-Writerを順番に呼び出す
   */
  @Bean
  public Step step() {
    return stepBuilderFactory.get("step")
        // 処理するデータ型の指定(Fruit)と処理単位の指定(10件ずつ)を行う
        .<FruitInput, FruitOutput>chunk(10)
        // 処理対象データの読み込み
        .reader(this.reader())
        // データ加工処理
        .processor(this.processor())
        // データ書き込み処理
        .writer(this.writer()).build();
  }

  /**
   * Reader<br>
   * CSVのデータを読み込み、stepへ返却する。
   */
  @Bean
  public FlatFileItemReader<FruitInput> reader() {

    // ReaderクラスにCSVの内容を読み込ませる
    FlatFileItemReader<FruitInput> reader = new FlatFileItemReader<FruitInput>();
    reader.setResource(new ClassPathResource("fruit_price.csv"));

    // CSVデータをFruitInputモデルに変換(フィールド名との紐付け)
    reader.setLineMapper(new DefaultLineMapper<FruitInput>() {
      {
        setLineTokenizer(new DelimitedLineTokenizer() {
          {
            setNames(new String[] { "name", "price" });
          }
        });
        setFieldSetMapper(new BeanWrapperFieldSetMapper<FruitInput>() {
          {
            setTargetType(FruitInput.class);
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
  public ItemProcessor<FruitInput, FruitOutput> processor() {
    // FruitItemProcessorでデータの加工処理
    return new FruitItemProcessor();
  }

  /**
   * Writer<br>
   * Processorで加工したデータを読み込みデータベースへの書き込みを行う
   */
  @Bean
  public JdbcBatchItemWriter<FruitOutput> writer() {
    JdbcBatchItemWriter<FruitOutput> writer = new JdbcBatchItemWriter<FruitOutput>();
    writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<FruitOutput>());
    writer.setDataSource(dataSource);

    // FruitOutputに定義したフィールド名でplaceHolder(:name)との紐付けを行い、データをバインドする
    writer.setSql("INSERT INTO FRUIT (NAME, PRICE, COMMENT) VALUES (:name, :price, :comment )");
    return writer;
  }
}
