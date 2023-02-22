package org.apache.orc.arcticfox;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class TestWriteStipeFooter {

    @Test
    public void testWriteIntoFile() {
        String filePathStr = "src/test/resources/test.orc";
        Configuration configuration = new Configuration();
        boolean ifSuccess = writeIntoFile(configuration, filePathStr);
        assert ifSuccess;
    }


    public boolean writeIntoFile(Configuration conf, String filePath) {
        try {
            TypeDescription schema =
                    TypeDescription.fromString("struct<x:int,y:string>");
            Writer writer = OrcFile.createWriter(new Path(filePath),
                    OrcFile.writerOptions(conf)
                            .setSchema(schema));
            VectorizedRowBatch batch = schema.createRowBatch();
            LongColumnVector x = (LongColumnVector) batch.cols[0];
            BytesColumnVector y = (BytesColumnVector) batch.cols[1];
            for(int r = 0; r < 10000; ++r) {
                int row = batch.size++;
                x.vector[row] = r;
                byte[] buffer = ("Last-" + (r * 3)).getBytes(StandardCharsets.UTF_8);
                y.setRef(row, buffer, 0, buffer.length);
                // If the batch is full, write it out and start over.
                if (batch.size == batch.getMaxSize()) {
                    writer.addRowBatch(batch);
                    batch.reset();
                }
            }
            if (batch.size != 0) {
                writer.addRowBatch(batch);
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

}
