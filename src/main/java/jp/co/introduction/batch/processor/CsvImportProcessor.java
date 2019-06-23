package jp.co.introduction.batch.processor;

import org.springframework.batch.item.ItemProcessor;

import jp.co.introduction.batch.model.FruitInsertModel;
import jp.co.introduction.batch.model.FruitReadCsvModel;

/**
 * Processor<br>
 * Readerで読み込んだデータの加工を行い、データベース書き込み用モデルを返却する
 */
public class CsvImportProcessor implements ItemProcessor<FruitReadCsvModel, FruitInsertModel> {

  @Override
  public FruitInsertModel process(FruitReadCsvModel fruit) throws Exception {
    // 大文字に変換
    String title = fruit.getName().toUpperCase();
    // そのまま取得
    int price = fruit.getPrice();

    return new FruitInsertModel(title, price);
  }
}
