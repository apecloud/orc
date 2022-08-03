package org.apache.orc.arcticfox;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.*;
import org.apache.orc.impl.OrcIndex;
import org.apache.orc.impl.OrcTail;
import org.apache.orc.impl.ReaderImpl;
import org.apache.orc.impl.RecordReaderImpl;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class TestPrintOrcStripeFooter {
    private static final int BATCH_SIZE = 1000;

    @Test
    public void testReadStripeFooterAndOrcIndex() {
        String filePathStr = "src/test/resources/lineitem.orc";
        List<Object> orcIndexStripeFooterOrcTail = getRecordReaderImpl(filePathStr);
        boolean ifSuccess =  initRecordReaderWithPreInited(filePathStr,
                (OrcTail) orcIndexStripeFooterOrcTail.get(0),
                (OrcProto.StripeFooter) orcIndexStripeFooterOrcTail.get(1),
                (OrcIndex) orcIndexStripeFooterOrcTail.get(2));
        assert ifSuccess;
    }

    // return <OrcIndex, StripeFooter, OrcTail>
    public List<Object> getRecordReaderImpl(String filePathStr) {
        Path filePath = new Path(filePathStr);
        List<Object> res = null;
        RecordReaderImpl recordReaderImpl;
        try {
            Reader reader = OrcFile.createReader(filePath, OrcFile.readerOptions(new Configuration()));
            TypeDescription readSchema = reader.getSchema();
            RecordReader recordReader = reader.rows(reader.options()
                    .schema(readSchema));
            assert recordReader != null;
            recordReaderImpl = (RecordReaderImpl) recordReader;

            int columnCount = readSchema.getMaximumId() + 1;
            boolean[] readCols = new boolean[columnCount];
            Arrays.fill(readCols, true);
            OrcIndex orcIndex = recordReaderImpl.readRowIndex(0, null, readCols);
            OrcProto.StripeFooter stripeFooter = recordReaderImpl.readFirstStripeFooter();
            OrcTail orcTail = ((ReaderImpl) reader).getOrcTail();
            res = Arrays.asList(orcTail, stripeFooter, orcIndex);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return res;
    }

    public boolean initRecordReaderWithPreInited(String filePathStr, OrcTail orcTail, OrcProto.StripeFooter stripeFooter,
                                                 OrcIndex orcIndex) {
        Path path = new Path(filePathStr);
        Configuration configuration = new Configuration();
        try {
            long startTime = System.currentTimeMillis();
            Reader reader = OrcFile.createReader(path,
                    OrcFile.readerOptions(configuration)
                            .filesystem(path.getFileSystem(configuration))
                            .orcTail(orcTail)
                            .stripeFooter(stripeFooter)
                            .orcIndex(orcIndex));
            TypeDescription readSchema = reader.getSchema();
            RecordReader recordReader = reader.rows(reader.options()
                    .schema(readSchema));
            System.out.println("Record reader has been created.");
            long createRecordReaderTime = System.currentTimeMillis() - startTime;
            startTime = System.currentTimeMillis();
            VectorizedRowBatch batch = readSchema.createRowBatch(BATCH_SIZE);
            int batchCount = 0;
            while (recordReader.nextBatch(batch)) {
                batchCount++;
            }
            long readBatchTime = System.currentTimeMillis() - startTime;
            recordReader.close();
            System.out.println("Total row count [" + reader.getNumberOfRows() + "]");
            System.out.println("Read batch size [" + BATCH_SIZE + "]");
            System.out.println("Read batch count [" + batchCount + "]");
            System.out.println("Create record reader time [" + createRecordReaderTime + "] ms.");
            System.out.println("Read orc batch time [" + readBatchTime + "] ms.");
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
