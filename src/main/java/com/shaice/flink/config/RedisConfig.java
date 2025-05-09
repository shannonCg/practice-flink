package com.shaice.flink.config;

import lombok.Getter;
import lombok.Setter;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.config.Config;

@Getter
@Setter
public class RedisConfig {
    private String redisMasterAddr;
    private String redisSlaveAddr;
    private String redisUserName;
    private String redisPassword;
    private String redisRetryAttempts;
    private String redisConnectTimeout;

    public Config getRedissonConfig() {

        Config config = new Config();

        config.useMasterSlaveServers().setMasterAddress(this.redisMasterAddr);
        config.useMasterSlaveServers().addSlaveAddress(this.redisSlaveAddr.split(","));
        config.useMasterSlaveServers().setUsername(this.redisUserName);
        config.useMasterSlaveServers().setPassword(this.redisPassword);

        config.useMasterSlaveServers().setRetryAttempts(Integer.parseInt(this.redisRetryAttempts));
        config.useMasterSlaveServers().setTimeout(Integer.parseInt(this.redisConnectTimeout));

        config.setCodec(new JsonJacksonCodec());
        return config;

    }
}
