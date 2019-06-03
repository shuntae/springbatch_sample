package jp.co.introduction.batch.processor;

import org.springframework.batch.item.ItemProcessor;

import jp.co.introduction.batch.model.FruitInput;
import jp.co.introduction.batch.model.FruitOutput;

/**
 * Processor<br>
 * Readerで読み込んだデータの加工を行い、データベース書き込み用モデルを返却する
 */
public class FruitItemProcessor implements ItemProcessor<FruitInput, FruitOutput> {

  @Override
  public FruitOutput process(FruitInput fruit) throws Exception {
    // 大文字に変換
    String title = fruit.getName().toUpperCase();
    // そのまま取得
    int price = fruit.getPrice();
    // 金額によってコメントを変える
    String comment = (price >= 1000) ? "高い！" : "お手頃価格";

    return new FruitOutput(title, price, comment);
  }
}
