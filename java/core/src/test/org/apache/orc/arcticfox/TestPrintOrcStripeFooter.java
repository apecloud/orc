package org.apache.orc.arcticfox;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.*;
import org.apache.orc.impl.OrcTail;
import org.apache.orc.impl.ReaderImpl;
import org.apache.orc.impl.RecordReaderImpl;
import org.junit.jupiter.api.Test;


public class TestPrintOrcStripeFooter {
    private static final int BATCH_SIZE = 1000;
    private static final String ORC_FILE_PATH = "src/test/resources/lineitem.orc";

    @Test
    public void testReadStripeFooterAndOrcIndex() {
        List<Object> orcIndexStripeFooterOrcTail = getRecordReaderImpl(ORC_FILE_PATH);
        boolean ifSuccess =  initRecordReaderWithPreInited(ORC_FILE_PATH,
                (OrcTail) orcIndexStripeFooterOrcTail.get(0),
                (OrcProto.StripeFooter) orcIndexStripeFooterOrcTail.get(1),
                (OrcProto.RowIndex[]) orcIndexStripeFooterOrcTail.get(2));
        assert ifSuccess;
    }

    @Test
    public void testSerializeStripeFooter() {
        List<Object> orcIndexStripeFooterOrcTail = getRecordReaderImpl(ORC_FILE_PATH);
        OrcProto.StripeFooter stripeFooter = (OrcProto.StripeFooter) orcIndexStripeFooterOrcTail.get(1);

        ByteBuffer byteBuffer = SerializeUtils.serializeStripeFooter(stripeFooter);
        OrcProto.StripeFooter deserializedStripeFooter = null;
        try {
            deserializedStripeFooter = SerializeUtils.deserializeStripeFooter(byteBuffer);
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
        assert stripeFooter.equals(deserializedStripeFooter);
    }

    @Test
    public void testSerializeRowIndex() {
        List<Object> orcIndexStripeFooterOrcTail = getRecordReaderImpl(ORC_FILE_PATH);
        OrcTail orcTail = (OrcTail) orcIndexStripeFooterOrcTail.get(0);
        orcTail.getStripes().get(0).getOffset();
        OrcProto.StripeFooter stripeFooter = (OrcProto.StripeFooter) orcIndexStripeFooterOrcTail.get(1);
        OrcProto.RowIndex[] rowGroupIndex = (OrcProto.RowIndex[]) orcIndexStripeFooterOrcTail.get(2);

        ByteBuffer byteBuffer = SerializeUtils.serializeRowIndex(rowGroupIndex);
        OrcProto.RowIndex[] rowGroupIndexSerialize = null;
        try {
            rowGroupIndexSerialize = SerializeUtils.deserializeRowIndex(byteBuffer, stripeFooter.getColumnsCount());
        } catch (IOException e) {
            e.printStackTrace();
        }

        assert Arrays.equals(rowGroupIndex, rowGroupIndexSerialize);
    }

    // return <OrcTail, StripeFooter, RowIndex[]>
    public List<Object> getRecordReaderImpl(String filePathStr) {
        Path filePath = new Path(filePathStr);
        List<Object> res = null;
        try {
            Reader reader = OrcFile.createReader(filePath, OrcFile.readerOptions(new Configuration()));
            TypeDescription readSchema = reader.getSchema();
            RecordReader recordReader = reader.rows(reader.options()
                    .schema(readSchema));
            assert recordReader != null;
            RecordReaderImpl recordReaderImpl = (RecordReaderImpl) recordReader;

            OrcTail orcTail = ((ReaderImpl) reader).getOrcTail();
            OrcProto.RowIndex[] rowGroupIndex = recordReaderImpl.getRowGroupIndex(0);
            OrcProto.StripeFooter stripeFooter = recordReaderImpl.getStripeFooter(0);
            res = Arrays.asList(orcTail, stripeFooter, rowGroupIndex);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return res;
    }

    public boolean initRecordReaderWithPreInited(String filePathStr, OrcTail orcTail, OrcProto.StripeFooter stripeFooter,
                                                 OrcProto.RowIndex[] rowGroupIndex) {
        Path path = new Path(filePathStr);
        Configuration configuration = new Configuration();
        try {
            long startTime = System.currentTimeMillis();
            Reader reader = OrcFile.createReader(path,
                    OrcFile.readerOptions(configuration)
                            .filesystem(path.getFileSystem(configuration))
                            .orcTail(orcTail)
                            .stripeFooter(stripeFooter)
                            .rowGroupIndex(rowGroupIndex));
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
