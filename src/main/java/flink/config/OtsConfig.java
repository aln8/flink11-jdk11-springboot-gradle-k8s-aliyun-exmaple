package flink.config;

import lombok.Data;

import java.io.Serializable;

@Data
public class OtsConfig implements Serializable {
    private String endPoint;
    private String accessKey;
    private String secretKey;
    private String instanceName;
    private String tableName;
}
