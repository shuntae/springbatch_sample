package jp.co.introduction.batch.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * データベース書き込み用クラス
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FruitOutput {

  private String name;
  private int price;
  private String comment;
}
