package com.batch.batchwriters.config;

import com.batch.batchwriters.listener.ProductSkipListener;
import com.batch.batchwriters.model.Product;
import com.batch.batchwriters.processor.ProductProcessor;
import com.batch.batchwriters.reader.ProductServiceAdaptor;
import com.batch.batchwriters.writer.ConsolePrintTasklet;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.item.adapter.ItemReaderAdapter;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.*;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FlatFileFormatException;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.oxm.xstream.XStreamMarshaller;
import org.springframework.web.client.ResourceAccessException;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.Writer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

@EnableBatchProcessing
@Configuration
public class BatchConfig {
    @Autowired
    private JobBuilderFactory jobs;

    @Autowired
    private StepBuilderFactory steps;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private ProductServiceAdaptor adaptor;

    @Bean
    public ItemReaderAdapter serviceReader(){
        ItemReaderAdapter reader = new ItemReaderAdapter();
        reader.setTargetObject(adaptor);
        reader.setTargetMethod("nextProduct");
        return reader;
    }



    @StepScope
    @Bean
    public FlatFileItemReader flatFileItemReader(
            @Value("#{jobParameters['inputFile']}")FileSystemResource fileSystemResource
            ){
        FlatFileItemReader reader = new FlatFileItemReader();
        reader.setResource(fileSystemResource);
        reader.setLinesToSkip(1);
        reader.setLineMapper(new DefaultLineMapper(){
            {
                setLineTokenizer(new DelimitedLineTokenizer(){
                    {
                        //must be in order as per file or parsing error
                        setNames(new String[]{"productId","productName","productDesc","price","unit"});
                        setDelimiter(",");
                    }
                });
                setFieldSetMapper(new BeanWrapperFieldSetMapper(){
                    {
                        setTargetType(Product.class);
                    }
                });
            }
        });

        return  reader;
    }


    @StepScope
    @Bean
    public FlatFileItemWriter flatFileItemWriter(
            @Value("#{jobParameters['outputFile']}")FileSystemResource fileSystemResource
    ){
        /*
        for showing the while writing if any exception occurs
        FlatFileItemWriter writer = new FlatFileItemWriter<Product>(){
            @Override
            public String doWrite(List<? extends Product> items) {
                for(Product p : items){
                    if(p.getProductId() ==9)
                        throw new RuntimeException("write exception because id is 9");
                }
                return super.doWrite(items);
            };
        };
         */
        FlatFileItemWriter writer = new FlatFileItemWriter();
        writer.setResource(fileSystemResource);
        writer.setLineAggregator(new DelimitedLineAggregator(){
            {
                setDelimiter(",");
                setFieldExtractor(new BeanWrapperFieldExtractor(){
                    {
                        //in the same order it will write
                        setNames(new String[]{"productId","prodName","productDesc","price","unit"});
                    }
                });
            }
        });

        //adding header line
        writer.setHeaderCallback(new FlatFileHeaderCallback() {
            @Override
            public void writeHeader(Writer writer) throws IOException {
                writer.write("productId,productName,productDesc,price,unit");
            }
        });

        //every time it overrides the output file so to prevent the same
        writer.setAppendAllowed(false); // false - overrides


        //adding footer
        /*
        writer.setFooterCallback(new FlatFileFooterCallback() {
            @Override
            public void writeFooter(Writer writer) throws IOException {
                writer.write("this file was updated at "+new SimpleDateFormat().format(new Date()));
            }
        });
         */
        return  writer;
    }

    @StepScope
    @Bean
    public StaxEventItemWriter xmlWriter( @Value("#{jobParameters['outputFile']}")FileSystemResource fileSystemResource){
        StaxEventItemWriter writer = new StaxEventItemWriter();
        HashMap<String,Class> aliases = new HashMap<>();
        aliases.put("product",Product.class);
        XStreamMarshaller marshaller = new XStreamMarshaller();
        marshaller.setAliases(aliases);
        marshaller.setAutodetectAnnotations(true);
        writer.setResource(fileSystemResource);
        writer.setMarshaller(marshaller);
        writer.setRootTagName("products");
        return writer;
    }


    @Bean
    public JdbcBatchItemWriter DbWriter(){

        JdbcBatchItemWriter writer = new JdbcBatchItemWriter();
        writer.setDataSource(dataSource);
        writer.setSql("insert into spring.product (prod_id, prod_name, prod_desc, price, unit) values (?,?,?,?,?)");
        writer.setItemPreparedStatementSetter(new ItemPreparedStatementSetter<Product>() {
            @Override
            public void setValues(Product o, PreparedStatement ps) throws SQLException {
                ps.setInt(1,o.getProductId());
                ps.setString(2,o.getProdName());
                ps.setString(3,o.getProductDesc());
                ps.setBigDecimal(4,o.getPrice());
                ps.setInt(5,o.getUnit());
            }
        });

        return writer;
    }

    //0r if two many columns use name mapping :FieldName

   /* @Bean
    public JdbcBatchItemWriter DbWriter2(){

      return new JdbcBatchItemWriterBuilder<Product>()
              .dataSource(dataSource)
              .sql("")
              .beanMapped().build();
    }*/


    @Bean
    public Step step0(){
        return steps.get("step0")
                .tasklet(new ConsolePrintTasklet())
                .build();
    }


    @Bean
    public Step step1(){
        return steps.get("step1")
                .<Product,Product>chunk(3)
                .reader(flatFileItemReader(null))
                //.reader(serviceReader())
                .processor(new ProductProcessor())
                //.writer(flatFileItemWriter(null))
                //.writer(xmlWriter(null))
                .writer(DbWriter())
                .faultTolerant()
                //.retry(ResourceAccessException.class)
                //.retryLimit(5)
                .skip(RuntimeException.class)
                .skipLimit(30)
                //.skipPolicy(new AlwaysSkipItemSkipPolicy())
                //.listener(new ProductSkipListener())
                .build();
    }


    @Bean
    public Job job1(){
        return jobs.get("job1")
                //below indicates each time create new job instance
                .incrementer(new RunIdIncrementer())
                .start(step0())
                .next(step1())
                .build();
    }

}
