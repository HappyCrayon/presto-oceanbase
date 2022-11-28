/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.oceanbase;

import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.plugin.jdbc.BaseJdbcClient;
import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.ConnectionFactory;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcErrorCode;
import com.facebook.presto.plugin.jdbc.JdbcIdentity;
import com.facebook.presto.plugin.jdbc.JdbcTypeHandle;
import com.facebook.presto.plugin.jdbc.ReadMapping;
import com.facebook.presto.plugin.jdbc.StandardReadMappings;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;
import com.oceanbase.jdbc.Driver;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;

/**
 * Implementation of OracleClient. It describes table, schemas and columns behaviours.
 * It allows to change the QueryBuilder to a custom one as well.
 *
 * @author Marcelo Paes Rech
 */
public class OceanBaseClient extends BaseJdbcClient {

    private static final int FETCH_SIZE = 1000;
    private final boolean synonymsEnabled;
    private final int numberDefaultScale;

    @Inject
    public OceanBaseClient(JdbcConnectorId connectorId, BaseJdbcConfig config, OceanBaseConfig oceanBaseConfig) throws SQLException {
        super(connectorId, config, "`", connectionFactory(config, oceanBaseConfig));
        this.synonymsEnabled = oceanBaseConfig.isSynonymsEnabled();
        this.numberDefaultScale = oceanBaseConfig.getNumberDefaultScale();
    }

    private static ConnectionFactory connectionFactory(BaseJdbcConfig config, OceanBaseConfig oceanBaseConfig) throws SQLException {
        Properties connectionProperties = DriverConnectionFactory.basicConnectionProperties(config);
        connectionProperties.setProperty("useInformationSchema", "true");
        connectionProperties.setProperty("nullCatalogMeansCurrent", "false");
        connectionProperties.setProperty("useUnicode", "true");
        connectionProperties.setProperty("characterEncoding", "utf8");
        connectionProperties.setProperty("tinyInt1isBit", "false");
        return new DriverConnectionFactory(new Driver(), config.getConnectionUrl(), Optional.ofNullable(config.getUserCredentialName()), Optional.ofNullable(config.getPasswordCredentialName()), connectionProperties);
    }

//    @Inject
//    public OceanBaseClient(JdbcConnectorId connectorId, BaseJdbcConfig config, OceanBaseConfig oceanBaseConfig, ConnectionFactory connectionFactory) {
//        super(connectorId, config, "\"", connectionFactory);
//        Objects.requireNonNull(oceanBaseConfig, "oceanbase config is null");
//        this.synonymsEnabled = oceanBaseConfig.isSynonymsEnabled();
//        this.numberDefaultScale = oceanBaseConfig.getNumberDefaultScale();
//    }

    private String[] getTableTypes() {
        return this.synonymsEnabled ? new String[]{"TABLE", "VIEW", "SYNONYM"} : new String[]{"TABLE", "VIEW"};
    }

    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName) throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        String escape = metadata.getSearchStringEscape();
        return metadata.getTables(connection.getCatalog(), escapeNamePattern(schemaName, Optional.of(escape)).orElse(null), escapeNamePattern(tableName, Optional.of(escape)).orElse(null), this.getTableTypes());
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setFetchSize(FETCH_SIZE);
        return statement;
    }

    @Override
    protected String generateTemporaryTableName() {
        return "presto_tmp_" + System.nanoTime();
    }

    @Override
    protected void renameTable(JdbcIdentity identity, String catalogName, SchemaTableName oldTable, SchemaTableName newTable) {
        if (!oldTable.getSchemaName().equalsIgnoreCase(newTable.getSchemaName())) {
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Table rename across schemas is not supported in OceanBase");
        } else {
            String newTableName = newTable.getTableName().toUpperCase(Locale.ENGLISH);
            String oldTableName = oldTable.getTableName().toUpperCase(Locale.ENGLISH);
            String sql = String.format("ALTER TABLE %s RENAME TO %s", this.quoted(catalogName, oldTable.getSchemaName(), oldTableName), this.quoted(newTableName));

            try {
                Connection connection = this.connectionFactory.openConnection(identity);
                Throwable var9 = null;

                try {
                    this.execute(connection, sql);
                } catch (Throwable var19) {
                    var9 = var19;
                    throw var19;
                } finally {
                    if (connection != null) {
                        if (var9 != null) {
                            try {
                                connection.close();
                            } catch (Throwable var18) {
                                var9.addSuppressed(var18);
                            }
                        } else {
                            connection.close();
                        }
                    }

                }

            } catch (SQLException var21) {
                throw new PrestoException(JdbcErrorCode.JDBC_ERROR, var21);
            }
        }
    }

    @Override
    public Optional<ReadMapping> toPrestoType(ConnectorSession session, JdbcTypeHandle typeHandle) {
        int columnSize = typeHandle.getColumnSize();
        switch (typeHandle.getJdbcType()) {
            case Types.LONGVARCHAR:
                if (columnSize <= 2147483646 && columnSize != 0) {
                    return Optional.of(StandardReadMappings.varcharReadMapping(VarcharType.createVarcharType(columnSize)));
                }

                return Optional.of(StandardReadMappings.varcharReadMapping(VarcharType.createUnboundedVarcharType()));
            case Types.NUMERIC:
                int precision = columnSize == 0 ? 38 : columnSize;
                int scale = typeHandle.getDecimalDigits();
                if (scale == 0) {
                    return Optional.of(StandardReadMappings.bigintReadMapping());
                } else {
                    if (scale >= 0 && scale <= precision) {
                        return Optional.of(StandardReadMappings.decimalReadMapping(DecimalType.createDecimalType(precision, scale)));
                    }

                    return Optional.of(StandardReadMappings.decimalReadMapping(DecimalType.createDecimalType(precision, this.numberDefaultScale)));
                }
            case Types.SMALLINT:
                return Optional.of(StandardReadMappings.smallintReadMapping());
            case Types.FLOAT:
            case Types.DOUBLE:
                return Optional.of(StandardReadMappings.doubleReadMapping());
            case Types.REAL:
                return Optional.of(StandardReadMappings.realReadMapping());
            case Types.VARCHAR:
                return Optional.of(StandardReadMappings.varcharReadMapping(VarcharType.createVarcharType(columnSize)));
            case Types.CLOB:
                return Optional.of(StandardReadMappings.varcharReadMapping(VarcharType.createUnboundedVarcharType()));
            default:
                return super.toPrestoType(session, typeHandle);
        }
    }
}
