package org.example;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.internal.DistributionLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * A Spring Boot application bootstrapping a Pivotal GemFire Server in the JVM process.
 *
 * @author John Blum
 * @since 1.0.0
 */
@SpringBootApplication
@SuppressWarnings("unused")
public class SpringBootGemFireServer {

    protected static final int DEFAULT_CACHE_SERVER_PORT = CacheServer.DEFAULT_PORT;
    protected static final int DEFAULT_LOCATOR_PORT = DistributionLocator.DEFAULT_LOCATOR_PORT;
    protected static final int DEFAULT_MANAGER_PORT = 1199;

    protected static final String DEFAULT_LOG_LEVEL = "config";

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    public static void main(String[] args) {
        SpringApplication.run(SpringBootGemFireServer.class);
    }

    String applicationName() {
        return SpringBootGemFireServer.class.getSimpleName();
    }

    @Bean
    Properties gemfireProperties(@Value("${spring.gemfire.log-level:" + DEFAULT_LOG_LEVEL + "}") String logLevel,
                                 @Value("${spring.gemfire.locators:localhost[" + DEFAULT_LOCATOR_PORT + "]}") String locators,
                                 @Value("${spring.gemfire.manager.port:" + DEFAULT_MANAGER_PORT + "}") int managerPort,
                                 @Value("${spring.gemfire.manager.start:false}") boolean jmxManagerStart,
                                 @Value("${spring.gemfire.start-locator}") String startLocator) {

        logger.warn("spring.gemfire.log-level is [{}]", logLevel);

        Properties gemfireProperties = new Properties();

        gemfireProperties.setProperty("name", applicationName());
        gemfireProperties.setProperty("mcast-port", "0");
        gemfireProperties.setProperty("log-level", logLevel);
        gemfireProperties.setProperty("locators", locators);

        return gemfireProperties;
    }

    @Bean
    Cache gemfireCache(@Qualifier("gemfireProperties") Properties gemfireProperties) {
        return new CacheFactory(gemfireProperties).create();
    }

    @Bean
    CacheServer gemfireCacheServer(Cache gemfireCache,
                                              @Value("${spring.gemfire.cache.server.bind-address:localhost}") String bindAddress,
                                              @Value("${spring.gemfire.cache.server.hostname-for-clients:localhost}") String hostnameForClients,
                                              @Value("${spring.gemfire.cache.server.port:" + DEFAULT_CACHE_SERVER_PORT + "}") int port) throws IOException {


        CacheServer cacheServer = gemfireCache.addCacheServer();


        cacheServer.setBindAddress(bindAddress);
        cacheServer.setHostnameForClients(hostnameForClients);
        cacheServer.setMaximumTimeBetweenPings(Long.valueOf(TimeUnit.SECONDS.toMillis(15)).intValue());
        cacheServer.setPort(port);

        cacheServer.start();
        return cacheServer;
    }

    @Bean(name = "Factorials")
    Region factorialsRegion(Cache gemfireCache) {

        RegionFactory<Long, Long> factory = gemfireCache.createRegionFactory(RegionShortcut.PARTITION);

        factory.setKeyConstraint(Long.class);
        factory.setValueConstraint(Long.class);
        factory.setCacheLoader(factorialsCacheLoader());

        return factory.create("Factorials");
    }


    CacheLoader<Long, Long> factorialsCacheLoader() {
        return new CacheLoader<Long, Long>() {
            @Override
            public Long load(LoaderHelper<Long, Long> helper) throws CacheLoaderException {
                Long number = helper.getKey();

                Assert.notNull(number, "Number must not be null");
                Assert.isTrue(number >= 0, String.format("Number [%1$d] must be greater than equal to 0", number));

                if (number <= 2l) {
                    return (number < 2l ? 1l : 2l);
                }

                long result = number;

                while (number-- > 2l) {
                    result *= number;
                }

                return result;
            }

            @Override
            public void close() {
            }
        };
    }
}
