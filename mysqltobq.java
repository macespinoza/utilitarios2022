/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package pe.com.aprendizaje.mac_jdbctocs;

import com.google.api.services.bigquery.model.TableRow;
import java.util.TimeZone;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 *
 * @author Miguel Cotrina
 */
public class mysqltobq {
    public static void main(String[] args) {
        
        TimeZone timeZone = TimeZone.getTimeZone("America/Lima");
        TimeZone.setDefault(timeZone);
        System.setProperty("user.timezone", "America/Lima");
        CustomPipelineOptions.custompipelineoptions pipelineOptions =
                PipelineOptionsFactory.fromArgs(args).as(CustomPipelineOptions.custompipelineoptions .class);
        
        run(pipelineOptions);
    }
    
    private static PipelineResult run(CustomPipelineOptions.custompipelineoptions options) {
        Pipeline pipeline = Pipeline.create(options);
        /**/
        
       pipeline.apply("Extraccion de JDBC",JdbcIO.<TableRow>read()
            .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
            options.getDriverClassName(),options.getConnectionURL())

              .withUsername(options.getUsername())
              .withPassword(options.getPassword()))
              .withQuery(options.getQuery())
              .withCoder(TableRowJsonCoder.of())
              .withRowMapper(CustomPipelineOptions.getResultSetToTableRow())) 
               
              .apply(
            "Escritura en Bigquery",
            BigQueryIO.writeTableRows()
                .withoutValidation()
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory())
                .to(options.getOutputTable()));
       
         return pipeline.run();
    }
}
