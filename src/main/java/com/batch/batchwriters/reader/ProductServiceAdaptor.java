package com.batch.batchwriters.reader;

import com.batch.batchwriters.model.Product;
import com.batch.batchwriters.service.ProductService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ProductServiceAdaptor {

    Logger logger = LoggerFactory.getLogger(ProductServiceAdaptor.class);

    @Autowired
    ProductService productService;

    public Product nextProduct() throws InterruptedException {
        Product p = null;
        Thread.sleep(1000);

        try{
            logger.info("connect to web service ... ok");
            p = productService.getProduct();
        }catch (Exception e){
            logger.info("excepion --- "+e.getMessage());
            throw e;
        }

        return p;
    }
}
