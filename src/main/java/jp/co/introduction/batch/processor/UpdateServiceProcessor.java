package jp.co.introduction.batch.processor;

import org.springframework.batch.item.ItemProcessor;

import jp.co.introduction.batch.entity.FruitEntity;
import jp.co.introduction.batch.model.FruitUpdateModel;

/**
 * Processor<br>
 * Readerで読み込んだデータの加工を行い、データベース更新用モデルを返却する
 */
public class UpdateServiceProcessor implements ItemProcessor<FruitEntity, FruitUpdateModel> {

  @Override
  public FruitUpdateModel process(FruitEntity fruit) throws Exception {
    // そのまま取得
    int id = fruit.getId();
    // そのまま取得
    String title = fruit.getName();
    // そのまま取得
    int price = fruit.getPrice();
    // 金額によってコメントを変える
    String comment = (price >= 1000) ? "高い！" : "お手頃価格";

    return new FruitUpdateModel(id, title, price, comment);
  }
}
