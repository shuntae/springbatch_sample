package jp.co.introduction.batch.entity;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

public class FruitRowMapper implements RowMapper<FruitEntity> {
  @Override
  public FruitEntity mapRow(ResultSet row, int rowNum) throws SQLException {
    return new FruitEntity(row);
  }
}
