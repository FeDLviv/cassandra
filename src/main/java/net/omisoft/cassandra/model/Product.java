package net.omisoft.cassandra.model;

import com.datastax.driver.core.DataType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

import java.io.Serializable;
import java.util.UUID;

@Table("products")
@AllArgsConstructor
@NoArgsConstructor
@Data
public class Product implements Serializable {

    @PrimaryKey
    @CassandraType(type = DataType.Name.UUID)
    private UUID id;

    private String name;

    private int price;

}