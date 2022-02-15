package com.batch.batchwriters.processor;

import com.batch.batchwriters.model.Product;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

public class ProductProcessor implements ItemProcessor<Product, Product> {
    @Override
    public Product process(Product product) throws Exception {
        /*
        if(product.getProductId() == 2)
            //return null;
            throw new RuntimeException("because id is 2");
        else

         */
            product.setProductDesc(product.getProductDesc().toUpperCase());
        return product;
    }
}
