package com.datacenter.schemaregistry.core;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Created by zuoc on 2017/6/13.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({RedisSchemaStoreTest.class, SimpleSchemaRegistryTest.class})
public class AllTest {
}
