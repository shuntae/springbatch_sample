package jp.co.introduction.batch.entity;

import java.sql.ResultSet;
import java.sql.SQLException;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FruitEntity {

  /**
   * SQL実行結果の詰め替えを行う。
   *
   * @param rs SQL実行結果
   * @throws SQLException SQL例外
   */
  public FruitEntity(ResultSet rs) throws SQLException {
    this.setId(rs.getInt("id"));
    this.setName(rs.getString("name"));
    this.setPrice(rs.getInt("price"));
    this.setComment(rs.getString("comment"));
  }

  private int id;
  private String name;
  private int price;
  private String comment;
}
