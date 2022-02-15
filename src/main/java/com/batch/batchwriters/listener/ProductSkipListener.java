package com.batch.batchwriters.listener;

import org.springframework.batch.core.annotation.OnSkipInProcess;
import org.springframework.batch.core.annotation.OnSkipInRead;
import org.springframework.batch.core.annotation.OnSkipInWrite;
import org.springframework.batch.item.file.FlatFileParseException;

import java.io.FileOutputStream;
import java.io.IOException;

public class ProductSkipListener {

    private String errorFileName = "error/read_skipped.csv";
    private String processErrorFileName = "error/process_skipped.csv";
    private String writeErrorFileName = "error/write_skipped.csv";


    @OnSkipInRead
    public void skipInRead(Throwable t){
        if(t instanceof FlatFileParseException){
            FlatFileParseException ffpe = (FlatFileParseException)t;
            onSkip(ffpe.getInput(),errorFileName);
        }
    }

    @OnSkipInProcess
    public void skipInProcess(Object o,Throwable t){
       if(t instanceof  RuntimeException){
           onSkip(o,processErrorFileName);
       }
    }

    @OnSkipInWrite
    public void skipInWrite(Object o,Throwable t){
        if(t instanceof  RuntimeException){
            onSkip(o,writeErrorFileName);
        }
    }

    private void onSkip(Object input, String fname) {
        FileOutputStream fos = null;
        try{
            fos = new FileOutputStream(fname,true);
            fos.write(input.toString().getBytes());
            fos.write("\r\n".getBytes());
            fos.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
