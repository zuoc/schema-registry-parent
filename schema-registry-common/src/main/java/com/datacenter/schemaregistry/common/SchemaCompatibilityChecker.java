package com.datacenter.schemaregistry.common;

import org.apache.avro.Schema;
import org.apache.avro.SchemaValidationException;
import org.apache.avro.SchemaValidator;
import org.apache.avro.SchemaValidatorBuilder;

import java.util.Collections;
import java.util.List;

/**
 * 模式兼容性检查器
 * Created by zuoc on 2017/5/22.
 */
public class SchemaCompatibilityChecker {

    private static final SchemaValidator READ_VALIDATOR = new SchemaValidatorBuilder().canReadStrategy().validateLatest();
    protected static final SchemaCompatibilityChecker READ_CHECKER = new SchemaCompatibilityChecker(READ_VALIDATOR);

    private static final SchemaValidator WRITE_VALIDATOR = new SchemaValidatorBuilder().canBeReadStrategy().validateLatest();
    protected static final SchemaCompatibilityChecker WRITE_CHECKER = new SchemaCompatibilityChecker(WRITE_VALIDATOR);

    private static final SchemaValidator FULL_VALIDATOR = new SchemaValidatorBuilder().mutualReadStrategy().validateLatest();
    protected static final SchemaCompatibilityChecker FULL_CHECKER = new SchemaCompatibilityChecker(FULL_VALIDATOR);

    private static final SchemaValidator READ_TRANSITIVE_VALIDATOR = new SchemaValidatorBuilder().canReadStrategy().validateAll();
    protected static final SchemaCompatibilityChecker READ_TRANSITIVE_CHECKER = new SchemaCompatibilityChecker(READ_TRANSITIVE_VALIDATOR);

    private static final SchemaValidator WRITE_TRANSITIVE_VALIDATOR = new SchemaValidatorBuilder().canBeReadStrategy().validateAll();
    protected static final SchemaCompatibilityChecker WRITE_TRANSITIVE_CHECKER = new SchemaCompatibilityChecker(WRITE_TRANSITIVE_VALIDATOR);

    private static final SchemaValidator FULL_TRANSITIVE_VALIDATOR = new SchemaValidatorBuilder().mutualReadStrategy().validateAll();
    protected static final SchemaCompatibilityChecker FULL_TRANSITIVE_CHECKER = new SchemaCompatibilityChecker(FULL_TRANSITIVE_VALIDATOR);

    private static final SchemaValidator NO_OP_VALIDATOR = ((toValidate, existing) -> {/**do nothing*/});
    protected static final SchemaCompatibilityChecker NO_OP_CHECKER = new SchemaCompatibilityChecker(NO_OP_VALIDATOR);

    private final SchemaValidator validator;

    private SchemaCompatibilityChecker(SchemaValidator validator) {
        this.validator = validator;
    }

    protected boolean isCompatible(Schema newSchema, Schema latestSchema) {
        return isCompatible(newSchema, Collections.singletonList(latestSchema));
    }

    protected boolean isCompatible(Schema newSchema, List<Schema> previousSchemas) {
        try {
            validator.validate(newSchema, previousSchemas);
        } catch (SchemaValidationException e) {
            return false;
        }
        return true;
    }

}
