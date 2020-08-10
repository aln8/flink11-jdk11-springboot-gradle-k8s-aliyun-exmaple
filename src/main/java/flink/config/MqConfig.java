package flink.config;

import com.aliyun.openservices.ons.api.PropertyKeyConst;
import lombok.Data;

import java.io.Serializable;
import java.util.Properties;

@Data
public class MqConfig implements Serializable {
    private String accessKey;
    private String secretKey;
    private String endpoint;
    private String instanceID;
    private String topic;
    private String tag;
    private String group;

    public Properties RocketMqPropertie() {
        Properties properties = new Properties();
        properties.setProperty(PropertyKeyConst.AccessKey, this.accessKey);
        properties.setProperty(PropertyKeyConst.SecretKey, this.secretKey);
        properties.setProperty(PropertyKeyConst.NAMESRV_ADDR, this.endpoint);
        properties.setProperty(PropertyKeyConst.GROUP_ID, this.group);
        return properties;
    }
}
