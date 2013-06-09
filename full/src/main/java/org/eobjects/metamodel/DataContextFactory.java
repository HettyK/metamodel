/**
 * eobjects.org MetaModel
 * Copyright (C) 2010 eobjects.org
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, write to:
 * Free Software Foundation, Inc.
 * 51 Franklin Street, Fifth Floor
 * Boston, MA  02110-1301  USA
 */
package org.eobjects.metamodel;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.util.Collection;

import javax.sql.DataSource;

import org.ektorp.http.StdHttpClient.Builder;
import org.eobjects.metamodel.access.AccessDataContext;
import org.eobjects.metamodel.couchdb.CouchDbDataContext;
import org.eobjects.metamodel.csv.CsvConfiguration;
import org.eobjects.metamodel.csv.CsvDataContext;
import org.eobjects.metamodel.dbase.DbaseDataContext;
import org.eobjects.metamodel.excel.ExcelConfiguration;
import org.eobjects.metamodel.excel.ExcelDataContext;
import org.eobjects.metamodel.fixedwidth.FixedWidthConfiguration;
import org.eobjects.metamodel.fixedwidth.FixedWidthDataContext;
import org.eobjects.metamodel.jdbc.JdbcDataContext;
import org.eobjects.metamodel.mongodb.MongoDbDataContext;
import org.eobjects.metamodel.openoffice.OpenOfficeDataContext;
import org.eobjects.metamodel.salesforce.SalesforceDataContext;
import org.eobjects.metamodel.schema.TableType;
import org.eobjects.metamodel.sugarcrm.SugarCrmDataContext;
import org.eobjects.metamodel.util.FileHelper;
import org.eobjects.metamodel.xml.XmlDomDataContext;
import org.xml.sax.InputSource;

import com.mongodb.DB;
import com.mongodb.Mongo;

/**
 * A factory for DataContext objects. This class substantially easens the task
 * of creating and initializing DataContext objects and/or their strategies for
 * reading datastores.
 * 
 * @see DataContext
 */
public class DataContextFactory {

    public static final char DEFAULT_CSV_SEPARATOR_CHAR = CsvConfiguration.DEFAULT_SEPARATOR_CHAR;
    public static final char DEFAULT_CSV_QUOTE_CHAR = CsvConfiguration.DEFAULT_QUOTE_CHAR;

    private DataContextFactory() {
        // Prevent instantiation
    }

    /**
     * Creates a composite DataContext based on a set of delegate DataContexts.
     * 
     * Composite DataContexts enables cross-DataContext querying and unified
     * schema exploration
     * 
     * @param delegates
     *            an array/var-args of delegate DataContexts
     * @return a DataContext that matches the request
     */
    public static DataContext createCompositeDataContext(DataContext... delegates) {
        return new CompositeDataContext(delegates);
    }

    /**
     * Creates a composite DataContext based on a set of delegate DataContexts.
     * 
     * Composite DataContexts enables cross-DataContext querying and unified
     * schema exploration
     * 
     * @param delegates
     *            a collection of delegate DataContexts
     * @return a DataContext that matches the request
     */
    public static DataContext createCompositeDataContext(Collection<DataContext> delegates) {
        return new CompositeDataContext(delegates);
    }

    /**
     * Creates a DataContext based on a MS Access (.mdb) file
     * 
     * @param filename
     *            the path to a MS Access (.mdb) file
     * @return a DataContext object that matches the request
     */
    public static DataContext createAccessDataContext(String filename) {
        return new AccessDataContext(filename);
    }

    /**
     * Creates a DataContext based on a MS Access (.mdb) file
     * 
     * @param file
     *            a MS Access (.mdb) file
     * @return a DataContext object that matches the request
     */
    public static DataContext createAccessDataContext(File file) {
        return new AccessDataContext(file);
    }

    /**
     * Creates a DataContext based on a dBase file
     * 
     * @param file
     *            a dBase (.dbf) file
     * @return a DataContext object that matches the request
     */
    public static DataContext createDbaseDataContext(File file) {
        return new DbaseDataContext(file);
    }

    /**
     * Creates a DataContext based on a dBase file
     * 
     * @param filename
     *            the path to a dBase (.dbf) file
     * @return a DataContext object that matches the request
     */
    public static DataContext createDbaseDataContext(String filename) {
        return new DbaseDataContext(filename);
    }

    /**
     * Creates a DataContext that connects to a Salesforce.com instance.
     * 
     * @param username
     *            the Salesforce username
     * @param password
     *            the Salesforce password
     * @param securityToken
     *            the Salesforce security token
     * @return a DataContext object that matches the request
     */
    public static DataContext createSalesforceDataContext(String username, String password, String securityToken) {
        return new SalesforceDataContext(username, password, securityToken);
    }

    /**
     * Create a DataContext that connects to a SugarCRM system.
     * 
     * @param baseUrl
     *            the base URL of the system, e.g. http://localhost/sugarcrm
     * @param username
     *            the SugarCRM username
     * @param password
     *            the SugarCRM password
     * @param applicationName
     *            the name of the application you are connecting with
     * @return a DataContext object that matches the request
     */
    public static DataContext createSugarCrmDataContext(String baseUrl, String username, String password, String applicationName) {
        return new SugarCrmDataContext(baseUrl, username, password, applicationName);
    }

    /**
     * Creates a DataContext based on a CSV file
     * 
     * @param file
     *            a CSV file
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createCsvDataContext(File file) {
        return createCsvDataContext(file, DEFAULT_CSV_SEPARATOR_CHAR, DEFAULT_CSV_QUOTE_CHAR);
    }

    /**
     * Creates a DataContext based on a CSV file
     * 
     * @param file
     *            a CSV file
     * @param separatorChar
     *            the char to use for separating values
     * @param quoteChar
     *            the char used for quoting values (typically if they include
     *            the separator char)
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createCsvDataContext(File file, char separatorChar, char quoteChar) {
        return createCsvDataContext(file, separatorChar, quoteChar, FileHelper.DEFAULT_ENCODING);
    }

    /**
     * Creates a DataContext based on a CSV file
     * 
     * @param file
     *            a CSV file
     * @param separatorChar
     *            the char to use for separating values
     * @param quoteChar
     *            the char used for quoting values (typically if they include
     *            the separator char)
     * @param encoding
     *            the character encoding of the file
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createCsvDataContext(File file, char separatorChar, char quoteChar, String encoding) {
        CsvConfiguration configuration = new CsvConfiguration(CsvConfiguration.DEFAULT_COLUMN_NAME_LINE, encoding, separatorChar,
                quoteChar, CsvConfiguration.DEFAULT_ESCAPE_CHAR);
        CsvDataContext dc = new CsvDataContext(file, configuration);
        return dc;
    }

    /**
     * Creates a DataContext based on a CSV file
     * 
     * @param file
     *            a CSV file
     * @param configuration
     *            the CSV configuration to use
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createCsvDataContext(File file, CsvConfiguration configuration) {
        CsvDataContext dc = new CsvDataContext(file, configuration);
        return dc;
    }

    /**
     * Creates a DataContext based on CSV-content through an input stream
     * 
     * @param inputStream
     *            the input stream to read from
     * @param separatorChar
     *            the char to use for separating values
     * @param quoteChar
     *            the char used for quoting values (typically if they include
     *            the separator char)
     * @return a DataContext object that matches the request
     */
    public static DataContext createCsvDataContext(InputStream inputStream, char separatorChar, char quoteChar) {
        return createCsvDataContext(inputStream, separatorChar, quoteChar, FileHelper.DEFAULT_ENCODING);
    }

    /**
     * Creates a DataContext based on CSV-content through an input stream
     * 
     * @param inputStream
     *            the input stream to read from
     * @param separatorChar
     *            the char to use for separating values
     * @param quoteChar
     *            the char used for quoting values (typically if they include
     *            the separator char)
     * @return a DataContext object that matches the request
     */
    public static DataContext createCsvDataContext(InputStream inputStream, char separatorChar, char quoteChar, String encoding) {
        CsvConfiguration configuration = new CsvConfiguration(CsvConfiguration.DEFAULT_COLUMN_NAME_LINE, encoding, separatorChar,
                quoteChar, CsvConfiguration.DEFAULT_ESCAPE_CHAR);
        CsvDataContext dc = new CsvDataContext(inputStream, configuration);
        return dc;
    }

    /**
     * Creates a DataContext based on CSV-content through an input stream
     * 
     * @param inputStream
     *            the input stream to read from
     * @param configuration
     *            the CSV configuration to use
     * @return a DataContext object that matches the request
     */
    public static DataContext createCsvDataContext(InputStream inputStream, CsvConfiguration configuration) {
        CsvDataContext dc = new CsvDataContext(inputStream, configuration);
        return dc;
    }

    /**
     * Creates a DataContext based on a fixed width file.
     * 
     * @param file
     *            the file to read from.
     * @param fileEncoding
     *            the character encoding of the file.
     * @param fixedValueWidth
     *            the (fixed) width of values in the file.
     * @return a DataContext object that matches the request
     */
    public static DataContext createFixedWidthDataContext(File file, String fileEncoding, int fixedValueWidth) {
        return createFixedWidthDataContext(file, new FixedWidthConfiguration(FixedWidthConfiguration.DEFAULT_COLUMN_NAME_LINE,
                fileEncoding, fixedValueWidth));
    }

    /**
     * Creates a DataContext based on a fixed width file.
     * 
     * @param file
     *            the file to read from.
     * @param configuration
     *            the fixed width configuration to use
     * @return a DataContext object that matches the request
     */
    public static DataContext createFixedWidthDataContext(File file, FixedWidthConfiguration configuration) {
        FixedWidthDataContext dc = new FixedWidthDataContext(file, configuration);
        return dc;
    }

    /**
     * Creates a DataContext based on a fixed width file.
     * 
     * @param file
     *            the file to read from.
     * @param fileEncoding
     *            the character encoding of the file.
     * @param fixedValueWidth
     *            the (fixed) width of values in the file.
     * @param headerLineNumber
     *            the line number of the column headers.
     * @return a DataContext object that matches the request
     */
    public static DataContext createFixedWidthDataContext(File file, String fileEncoding, int fixedValueWidth,
            int headerLineNumber) {
        return createFixedWidthDataContext(file, new FixedWidthConfiguration(FixedWidthConfiguration.DEFAULT_COLUMN_NAME_LINE,
                fileEncoding, fixedValueWidth));
    }

    /**
     * Creates a DataContet based on an Excel spreadsheet file
     * 
     * @param file
     *            an excel spreadsheet file
     * @param configuration
     *            the configuration with metadata for reading the spreadsheet
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createExcelDataContext(File file, ExcelConfiguration configuration) {
        return new ExcelDataContext(file, configuration);
    }

    /**
     * Creates a DataContext based on an Excel spreadsheet file
     * 
     * @param file
     *            an Excel spreadsheet file
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createExcelDataContext(File file) {
        return createExcelDataContext(file, new ExcelConfiguration());
    }

    /**
     * Creates a DataContext based on XML-content from an input source.
     * 
     * Tables are created by examining the data in the XML file, NOT by reading
     * XML Schemas (xsd/dtd's). This enables compliancy with ALL xml formats but
     * also raises a risk that two XML files with the same format wont
     * nescesarily yield the same table model if some optional attributes or
     * tags are omitted in one of the files.
     * 
     * @param inputSource
     *            an input source feeding XML content
     * @param schemaName
     *            the name to be used for the main schema
     * @param autoFlattenTables
     *            a boolean indicating if MetaModel should flatten very simple
     *            table structures (where tables only contain a single
     *            data-carrying column) for greater usability of the generated
     *            table-based model
     * @return a DataContext object that matches the request
     */
    public static DataContext createXmlDataContext(InputSource inputSource, String schemaName, boolean autoFlattenTables) {
        XmlDomDataContext dc = new XmlDomDataContext(inputSource, schemaName, autoFlattenTables);
        return dc;
    }

    /**
     * Creates a DataContext based on XML-content from a File.
     * 
     * Tables are created by examining the data in the XML file, NOT by reading
     * XML Schemas (xsd/dtd's). This enables compliancy with ALL xml formats but
     * also raises a risk that two XML files with the same format wont
     * nescesarily yield the same table model if some optional attributes or
     * tags are omitted in one of the files.
     * 
     * @param file
     *            the File to use for feeding XML content
     * @param autoFlattenTables
     *            a boolean indicating if MetaModel should flatten very simple
     *            table structures (where tables only contain a single
     *            data-carrying column) for greater usability of the generated
     *            table-based model
     * @return a DataContext object that matches the request
     */
    public static DataContext createXmlDataContext(File file, boolean autoFlattenTables) {
        XmlDomDataContext dc = new XmlDomDataContext(file, autoFlattenTables);
        return dc;
    }

    /**
     * Creates a DataContext based on XML-content from a URL.
     * 
     * Tables are created by examining the data in the XML file, NOT by reading
     * XML Schemas (xsd/dtd's). This enables compliancy with ALL xml formats but
     * also raises a risk that two XML files with the same format wont
     * nescesarily yield the same table model if some optional attributes or
     * tags are omitted in one of the files.
     * 
     * @param url
     *            the URL to use for feeding XML content
     * @param autoFlattenTables
     *            a boolean indicating if MetaModel should flatten very simple
     *            table structures (where tables only contain a single
     *            data-carrying column) for greater usability of the generated
     *            table-based model
     * @return a DataContext object that matches the request
     */
    public static DataContext createXmlDataContext(URL url, boolean autoFlattenTables) {
        XmlDomDataContext dc = new XmlDomDataContext(url, autoFlattenTables);
        return dc;
    }

    /**
     * Creates a DataContext based on an OpenOffice.org database file.
     * 
     * @param file
     *            an OpenOffice.org database file
     * @return a DataContext object that matches the request
     */
    public static DataContext createOpenOfficeDataContext(File file) {
        return new OpenOfficeDataContext(file);
    }

    /**
     * Creates a DataContext based on a JDBC connection
     * 
     * @param connection
     *            a JDBC connection
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createJdbcDataContext(Connection connection) {
        return new JdbcDataContext(connection);
    }

    /**
     * Creates a DataContext based on a JDBC datasource
     * 
     * @param ds
     *            a JDBC datasource
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createJdbcDataContext(DataSource ds) {
        return new JdbcDataContext(ds);
    }

    /**
     * Creates a DataContext based on a JDBC connection
     * 
     * @param connection
     *            a JDBC connection
     * @param catalogName
     *            a catalog name to use
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createJdbcDataContext(Connection connection, String catalogName) {
        return new JdbcDataContext(connection, TableType.DEFAULT_TABLE_TYPES, catalogName);
    }

    /**
     * Creates a DataContext based on a JDBC connection
     * 
     * @param connection
     *            a JDBC connection
     * @param tableTypes
     *            the types of tables to include in the generated schemas
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createJdbcDataContext(Connection connection, TableType... tableTypes) {
        return new JdbcDataContext(connection, tableTypes, null);
    }

    /**
     * Creates a DataContext based on a JDBC connection
     * 
     * @param connection
     *            a JDBC connection
     * @param catalogName
     *            a catalog name to use
     * @param tableTypes
     *            the types of tables to include in the generated schemas
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createJdbcDataContext(Connection connection, String catalogName, TableType[] tableTypes) {
        return new JdbcDataContext(connection, tableTypes, catalogName);
    }

    /**
     * Creates a DataContext based on a JDBC datasource
     * 
     * @param ds
     *            a JDBC datasource
     * @param tableTypes
     *            the types of tables to include in the generated schemas
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createJdbcDataContext(DataSource ds, TableType... tableTypes) {
        return new JdbcDataContext(ds, tableTypes, null);
    }

    /**
     * Creates a DataContext based on a JDBC datasource
     * 
     * @param ds
     *            a JDBC datasource
     * @param catalogName
     *            a catalog name to use
     * @param tableTypes
     *            the types of tables to include in the generated schemas
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createJdbcDataContext(DataSource ds, String catalogName, TableType[] tableTypes) {
        return new JdbcDataContext(ds, tableTypes, catalogName);
    }

    /**
     * Creates a DataContext based on a JDBC datasource
     * 
     * @param ds
     *            a JDBC datasource
     * @param catalogName
     *            a catalog name to use
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createJdbcDataContext(DataSource ds, String catalogName) {
        return new JdbcDataContext(ds, TableType.DEFAULT_TABLE_TYPES, catalogName);
    }

    /**
     * Creates a new MongoDB datacontext.
     * 
     * @param hostname
     *            The hostname of the MongoDB instance
     * @param port
     *            the port of the MongoDB instance, or null if the default port
     *            should be used.
     * @param databaseName
     *            the name of the database
     * @param username
     *            the username, or null if unauthenticated access should be used
     * @param password
     *            the password, or null if unathenticated access should be used
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createMongoDbDataContext(String hostname, Integer port, String databaseName,
            String username, char[] password) {
        try {
            DB mongoDb;
            if (port == null) {
                mongoDb = new Mongo(hostname).getDB(databaseName);
            } else {
                mongoDb = new Mongo(hostname, port).getDB(databaseName);
            }
            if (username != null) {
                mongoDb.authenticate(username, password);
            }
            return new MongoDbDataContext(mongoDb);
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new IllegalStateException(e);
        }
    }

    /**
     * Creates a new CouchDB datacontext.
     * 
     * @param hostname
     *            The hostname of the CouchDB instance
     * @param port
     *            the port of the CouchDB instance, or null if the default port
     *            should be used.
     * @param username
     *            the username, or null if unauthenticated access should be used
     * @param password
     *            the password, or null if unathenticated access should be used
     * @return a DataContext object that matches the request
     */
    public static UpdateableDataContext createCouchDbDataContext(String hostname, Integer port, String username, String password) {

        Builder httpClientBuilder = new Builder();
        httpClientBuilder.host(hostname);
        if (port != null) {
            httpClientBuilder.port(port);
        }
        if (username != null) {
            httpClientBuilder.username(username);
        }
        if (password != null) {
            httpClientBuilder.password(password);
        }

        // increased timeouts (20 sec) - metamodel typically does quite some
        // batching so it might take a bit of time to provide a connection.
        httpClientBuilder.connectionTimeout(20000);
        httpClientBuilder.socketTimeout(20000);

        return new CouchDbDataContext(httpClientBuilder);
    }
}