package com.milvus.functions;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.sql.SQLException;

public class AnnFunction extends SqlFunction {

    public AnnFunction() {
        super("ANN",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.explicit(SqlTypeName.FLOAT),
                new SqlOperandTypeInference() {
                    @Override
                    public void inferOperandTypes(SqlCallBinding callBinding, RelDataType returnType, RelDataType[] operandTypes) {
                        RelDataTypeFactory typeFactory = callBinding.getTypeFactory();
                        if (callBinding.getOperandCount() != 2) {
                            throw new RuntimeException("函数 ann 需要 2 个参数，但实际传入 " + callBinding.getOperandCount() + " 个");
                        }
                        RelDataType floatType = typeFactory.createSqlType(SqlTypeName.FLOAT);
                        RelDataType floatArrayType = typeFactory.createArrayType(floatType, -1);
                        operandTypes[0] = floatArrayType;
                        operandTypes[1] = floatArrayType;
                    }
                },
                new SqlOperandTypeChecker(){

                    @Override
                    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
                        if (callBinding.getOperandCount() != 2) {
                            if (throwOnFailure) {
                                throw new RuntimeException("函数需要 2 个参数，但实际传入 " + callBinding.getOperandCount() + " 个");
                            }
                            return false;
                        }

                        for (int i = 0; i < 2; i++) {
                            RelDataType operandType = callBinding.getOperandType(i);

                            if (operandType.getSqlTypeName() != SqlTypeName.ARRAY) {
                                if (throwOnFailure) {
                                    throw new RuntimeException("参数 " + (i + 1) + " 必须是数组类型");
                                }
                                return false;
                            }

                            RelDataType componentType = operandType.getComponentType();
                            if (componentType == null || componentType.getSqlTypeName() != SqlTypeName.FLOAT) {
                                if (throwOnFailure) {
                                    throw new RuntimeException("参数 " + (i + 1) + " 必须是 float[] 类型（元素类型为 FLOAT）");
                                }
                                return false;
                            }
                        }

                        return true;
                    }

                    @Override
                    public SqlOperandCountRange getOperandCountRange() {
                        return SqlOperandCountRanges.of(2);
                    }

                    @Override
                    public String getAllowedSignatures(SqlOperator operator, String functionName) {
                        return functionName + "(float[], float[])";
                    }
                },
                SqlFunctionCategory.USER_DEFINED_FUNCTION);
    }

    @Override
    public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
        if (call.operandCount() != 2) {
            throw new IllegalArgumentException("ANN function requires exactly 2 arguments");
        }

        RelDataType type1 = validator.deriveType(scope, call.operand(0));
        RelDataType type2 = validator.deriveType(scope, call.operand(1));

        if (type1.getSqlTypeName() != SqlTypeName.ARRAY) {
            throw new IllegalArgumentException("First argument must be ARRAY type, got: " + type1);
        }
        if (type2.getSqlTypeName() != SqlTypeName.ARRAY) {
            throw new IllegalArgumentException("Second argument must be ARRAY type, got: " + type2);
        }

        // 验证数组元素类型为 FLOAT
        RelDataType elementType1 = type1.getComponentType();
        RelDataType elementType2 = type2.getComponentType();

        if (elementType1 != null && elementType1.getSqlTypeName() != SqlTypeName.FLOAT) {
            throw new IllegalArgumentException("First argument array must contain FLOAT elements");
        }
        if (elementType2 != null && elementType2.getSqlTypeName() != SqlTypeName.FLOAT) {
            throw new IllegalArgumentException("Second argument array must contain FLOAT elements");
        }

        return validator.getTypeFactory().createSqlType(SqlTypeName.FLOAT);
    }
}
